import argparse
import sys
import traceback
from datetime import datetime

from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession

import ks_crypto.lib.constants as C
import ks_crypto.lib.spark_utils as su
from ks_crypto import str2bool
from ks_crypto.lib.log_generation import get_logs
from ks_crypto.feature_engineering.feature_engineering import feature_engineering
from ks_crypto.feature_engineering.data_aggregation import data_aggregation_by_period


def main():
    app_name = "KSCRYPTO - Ingenieria de variables"

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
        input_tablename = args.input_tablename
        output_tablename = args.output_tablename

        log_detalle.info(sys.version)
        log_detalle.info('Parametros:')
        log_detalle.info('> input_tablename: {}'.format(input_tablename))
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

        # ----------------------------------------------------------------------------------------------------------
        # Ejecucion principal
        # ----------------------------------------------------------------------------------------------------------
        log_detalle.info('Inicio de la extracción del dataset...')

        tablon_df = read_input_table(spark, input_tablename, f_min, f_max)

        log_detalle.info("Conteo del tablon inicial {count}".format(count=tablon_df.count()))

        tablon_df = \
            tablon_df\
            .transform(feature_engineering)\
            .transform(data_aggregation_by_period)

        log_detalle.info("Conteo del tablon final {count}".format(count=tablon_df.count()))

        log_detalle.info("Inicio del guardado del dataset. Tabla Hive: {}".format(output_tablename))

        if drop_output_table:
            # Para crear una tabla nueva
            su.export_to_big_query(tablon_df, output_tablename, mode='overwrite',
                                   partition_by=C.BLOCK_TIMESTAMP_MONTH)
        else:
            # Para añadir a una tabla ya existente
            su.export_to_big_query(tablon_df, output_tablename, mode='append',
                                   partition_by=C.BLOCK_TIMESTAMP_MONTH)

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
    default_input_tablename = 'kschool-crypto:ks_crypto_dataset.transactions_flatten_filt'
    default_output_tablename = 'kschool-crypto:ks_crypto_dataset.transactions_flatten_filt_ft'
    default_drop_output_table = False

    parser = argparse.ArgumentParser(description='Filtrado del tablon con ingenieria de variables')
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
