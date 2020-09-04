import argparse
import sys
import traceback
from datetime import datetime

from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession
import torch
import torch.distributed as dist
import numpy as np
import random

import ks_crypto.lib.constants as C
import ks_crypto.lib.modeling_utils as mu
from ks_crypto import str2bool
from ks_crypto.lib.log_generation import get_logs
from ks_crypto.modeling.evolveGCN.torch_data_provider import TorchGraphDataProvider
from ks_crypto.modeling.evolveGCN.node_cls_tasker import NodeClsTasker
from ks_crypto.modeling.evolveGCN import splitter as sp, egcn_h, models as mls, cross_entropy as ce, trainer as tr


def main():
    app_name = "KSCRYPTO - Modelado"

    # ------------------------------------------------------------------------------------------------------------------
    # Configuracion de logging
    # ------------------------------------------------------------------------------------------------------------------
    log_ejecucion, log_detalle = get_logs(app_name)
    log_ejecucion.info('Inicio')

    exit_value = 0
    try:

        # --------------------------------------------------------------------------------------------------------------
        # Argumentos
        # --------------------------------------------------------------------------------------------------------------
        parser = get_configured_arg_parser()
        args = parser.parse_args()
        f_max = datetime.strptime(args.end_date, C.PY_YYYY_MM_DD_MASK)
        period_unit = args.period_unit
        num_periods = args.num_periods
        f_min = f_max - relativedelta(**{period_unit: num_periods})
        drop_output_table = args.drop_output_table
        hdfs_checkpoint_path = args.check_point
        temp_bucket_name = args.temp_bucket_name
        n_input_tablename = args.n_input_tablename
        t_input_tablename = args.t_input_tablename
        output_tablename = args.output_tablename

        log_detalle.info(sys.version)
        log_detalle.info('Parametros:')
        log_detalle.info('> t_input_tablename: {}'.format(t_input_tablename))
        log_detalle.info('> n_input_tablename: {}'.format(n_input_tablename))
        log_detalle.info('> output_tablename: {}'.format(output_tablename))
        log_detalle.info('> check_point: {}'.format(hdfs_checkpoint_path))
        log_detalle.info('> temp_bucket_name: {}'.format(temp_bucket_name))
        log_detalle.info('> f_max: {}'.format(f_max))
        log_detalle.info('> f_min: {}'.format(f_min))
        log_detalle.info('> period_unit: {}'.format(period_unit))
        log_detalle.info('> num_periods: {}'.format(num_periods))
        log_detalle.info('> drop_output_table: {}'.format(drop_output_table))

        # --------------------------------------------------------------------------------------------------------------
        # Conexion de Spark
        # --------------------------------------------------------------------------------------------------------------
        log_detalle.info('Obteniendo contexto de spark...')
        conf = SparkConf() \
            .set("spark.executorEnv.PYTHONHASHSEED", "0") \
            .set("spark.sql.shuffle.partitions", "2048") \
            .set("spark.driver.maxResultSize", "30G") \
            .set("temporaryGcsBucket", temp_bucket_name)
        sc = SparkContext(conf=conf)
        sc.setCheckpointDir(hdfs_checkpoint_path)
        spark = SparkSession.builder.appName(app_name).getOrCreate()

        log_detalle.info('Parametros de la configuracion de spark:')
        for key, value in conf.getAll():
            log_detalle.info('> {key}: {value}'.format(key=key, value=value))

        args_dic = C.EVOLVE_GCN_MODEL_PARAMS_DIC

        global rank, wsize, use_cuda

        args_dic['use_cuda'] = (torch.cuda.is_available() and args_dic['use_cuda'])
        args_dic['device'] = 'cpu'
        if args_dic['use_cuda']:
            args_dic['device'] = 'cuda'
        log_detalle.info("use CUDA:", args_dic['use_cuda'], "- device:", args_dic['device'])
        try:
            dist.init_process_group(backend='mpi')
            rank = dist.get_rank()
            wsize = dist.get_world_size()
            log_detalle.info('Hello from process {} (out of {})'.format(dist.get_rank(), dist.get_world_size()))
            if args_dic['use_cuda']:
                torch.cuda.set_device(rank)  # are we sure of the rank+1????
                log_detalle.info('using the device {}'.format(torch.cuda.current_device()))
        except:
            rank = 0
            wsize = 1
            log_detalle.info(('MPI backend not preset. Set process rank to {} (out of {})'.format(rank, wsize)))

        if args_dic['seed'] is None and args_dic['seed'] != 'None':
            seed = 123 + rank  # int(time.time())+rank
        else:
            seed = args_dic['seed']  # +rank

        np.random.seed(seed)
        random.seed(seed)
        torch.manual_seed(seed)
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
        args_dic['seed'] = seed
        args_dic['rank'] = rank
        args_dic['wsize'] = wsize

        args_dic = mu.build_random_hyper_params(args_dic)

        # --------------------------------------------------------------------------------------------------------------
        # Ejecucion principal
        # --------------------------------------------------------------------------------------------------------------
        log_detalle.info('Inicio de la extracción del dataset...')

        nodes_df = read_input_table(spark, n_input_tablename, f_min, f_max).drop(C.BLOCK_TIMESTAMP_MONTH)
        log_detalle.info("Conteo del tablon de nodos {count}".format(count=nodes_df.count()))

        transactions_df = read_input_table(spark, t_input_tablename, f_min, f_max).drop(C.BLOCK_TIMESTAMP_MONTH)
        log_detalle.info("Conteo del tablon de transacciones {count}".format(count=transactions_df.count()))

        # build the dataset
        dataset = TorchGraphDataProvider(nodes_df, transactions_df)

        # build the tasker
        tasker = NodeClsTasker(args_dic=args_dic, dataset=dataset)

        # build the splitter
        splitter = sp.splitter(args_dic, tasker)

        # build the models
        gcn = egcn_h.EGCN(args_dic, activation=torch.nn.RReLU(), device=args_dic['device'])

        mult = 1
        in_feats = args_dic['layer_2_feats'] * mult
        classifier = mls.Classifier(args_dic,
                                    in_features=in_feats,
                                    out_features=tasker.num_classes).to(args_dic['device'])

        # build a loss
        cross_entropy = ce.Cross_Entropy(args_dic).to(args_dic['device'])

        # trainer
        trainer = tr.Trainer(args_dic,
                             splitter=splitter,
                             gcn=gcn,
                             classifier=classifier,
                             comp_loss=cross_entropy,
                             dataset=dataset,
                             num_classes=tasker.num_classes,
                             logging=log_detalle,
                             spark=spark,
                             drop_output_table=drop_output_table)

        trainer.train()

        # --------------------------------------------------------------------------------------------------------------
        # Cerramos conexion de Spark
        # --------------------------------------------------------------------------------------------------------------
        spark.sparkContext.stop()

    except:
        tb = traceback.format_exc()
        log_ejecucion.error(tb)
        exit_value = 1

    finally:
        log_ejecucion.info('Fin')
        exit(exit_value)


def get_configured_arg_parser():
    default_input_tablename = 'kschool-crypto:ks_crypto_dataset.transactions_flatten'
    default_output_tablename = 'kschool-crypto:ks_crypto_dataset.transactions_flatten_filt'
    default_drop_output_table = False

    parser = argparse.ArgumentParser(description='Filtrado del tablon aplanado')
    parser.add_argument('-i', '--input_tablename',
                        default=default_input_tablename,
                        help="el nombre completo de la tabla de entrada")
    parser.add_argument('-o', '--output_tablename',
                        default=default_output_tablename,
                        help="el nombre completo de la tabla de salida")
    parser.add_argument('-c', '--check_point',
                        help='la ruta en HDFS donde almacenar los checkpoints')
    parser.add_argument('-t', '--temp_bucket_name',
                        help='bucket temporal necesario para escritura')
    parser.add_argument('-e', '--end_date',
                        help='la fecha de finalización del periodo de extracción (no incluida)')
    parser.add_argument('-n', '--num_periods', type=int,
                        help='número de periodos de histórico a extraer')
    parser.add_argument('-u', '--period_unit',
                        help='unidad para los periodos (years, months, days...), '
                             'basandonos en relativedelta')
    parser.add_argument('-d', '--drop_output_table', type=str2bool,
                        default=default_drop_output_table,
                        help='si la tabla de salida existe, borramos la tabla (True) '
                             'o añadimos los nuevos datos (False)?')

    return parser


def read_input_table(spark, input_tablename, f_min, f_max):
    f_min = f_min.strftime(C.PY_YYYY_MM_DD_MASK)
    f_max = (f_max - relativedelta(days=1)).strftime(C.PY_YYYY_MM_DD_MASK)

    output_df = \
        spark.read.format('bigquery') \
        .option('table', input_tablename) \
        .option("filter", f"block_timestamp_month >= '{f_min}' AND block_timestamp_month < '{f_max}'") \
        .load()

    return output_df


if __name__ == "__main__":
    main()
