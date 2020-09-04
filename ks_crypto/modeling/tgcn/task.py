import argparse
import sys
import traceback
from datetime import datetime

from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

import ks_crypto.lib.constants as C
import ks_crypto.lib.modeling_utils as mu
from ks_crypto import str2bool
from ks_crypto.lib.log_generation import get_logs

# Add modeling libraries


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
        t_input_tablename = args.t_input_tablename
        n_input_tablename = args.n_input_tablename
        model_cluster_path = args.model_cluster_path

        log_detalle.info(sys.version)
        log_detalle.info('Parametros:')
        log_detalle.info('> t_input_tablename: {}'.format(t_input_tablename))
        log_detalle.info('> n_input_tablename: {}'.format(n_input_tablename))
        log_detalle.info('> check_point: {}'.format(hdfs_checkpoint_path))
        log_detalle.info('> temp_bucket_name: {}'.format(temp_bucket_name))
        log_detalle.info('> f_max: {}'.format(f_max))
        log_detalle.info('> f_min: {}'.format(f_min))
        log_detalle.info('> period_unit: {}'.format(period_unit))
        log_detalle.info('> num_periods: {}'.format(num_periods))
        log_detalle.info('> drop_output_table: {}'.format(drop_output_table))
        log_detalle.info('> model_cluster_path: {}'.format(model_cluster_path))

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

        # --------------------------------------------------------------------------------------------------------------
        # Ejecucion principal
        # --------------------------------------------------------------------------------------------------------------
        log_detalle.info('Inicio de la extracción del dataset...')

        nodes_df = read_input_table(spark, n_input_tablename, f_min, f_max).drop(C.BLOCK_TIMESTAMP_MONTH)
        log_detalle.info("Conteo del tablon de nodos {count}".format(count=nodes_df.count()))

        transactions_df = read_input_table(spark, t_input_tablename, f_min, f_max).drop(C.BLOCK_TIMESTAMP_MONTH)
        log_detalle.info("Conteo del tablon de transacciones {count}".format(count=transactions_df.count()))

        # Get adjacency_matrix

        # Split train and test

        # Scale data

        # Sequence data preparation

        # Create model

        # Train model

        # Save model

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
    default_t_input_tablename = 'kschool-crypto:ks_crypto_dataset.transactions_ft'
    default_n_input_tablename = 'kschool-crypto:ks_crypto_dataset.nodes_ft'
    default_model_cluster_path = '/home/tgcn_model'
    default_drop_output_table = False

    parser = argparse.ArgumentParser(description='Filtrado del tablon aplanado')
    parser.add_argument('-t', '--t_input_tablename',
                        default=default_t_input_tablename,
                        help="el nombre completo de la tabla de entrada")
    parser.add_argument('-i', '--n_input_tablename',
                        default=default_n_input_tablename,
                        help="el nombre completo de la tabla de entrada")
    parser.add_argument('-m', '--model_cluster_path',
                        default=default_model_cluster_path,
                        help="ruta en el cluster para la escritura del modelo ")
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
