import argparse
import sys
import traceback
from datetime import datetime

from dateutil.relativedelta import relativedelta
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession

import ks_crypto.lib.constants as C
import ks_crypto.lib.spark_utils as su
import ks_crypto.lib.utils as ut
from ks_crypto import str2bool
from ks_crypto.lib.log_generation import get_logs
from ks_crypto.extract_data.extract_danon_transactions import extract_danon_transactions
from ks_crypto.extract_data.extract_transactions import extract_transactions
from ks_crypto.extract_data.extract_anon_transactions_classes import extract_anon_transactions_classes


def main():
    app_name = "KSCRYPTO - Generacion tablon aplanado"

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
        periods_per_batch = args.periods_per_batch
        drop_output_table = args.drop_output_table
        hdfs_checkpoint_path = args.check_point
        output_tablename = args.output_tablename

        log_detalle.info(sys.version)
        log_detalle.info('Parametros:')
        log_detalle.info('> output_tablename: {}'.format(output_tablename))
        log_detalle.info('> check_point: {}'.format(hdfs_checkpoint_path))
        log_detalle.info('> f_max: {}'.format(f_max))
        log_detalle.info('> f_min: {}'.format(f_min))
        log_detalle.info('> period_unit: {}'.format(period_unit))
        log_detalle.info('> num_periods: {}'.format(num_periods))
        log_detalle.info('> periods_per_batch: {}'.format(periods_per_batch))
        log_detalle.info('> drop_output_table: {}'.format(drop_output_table))

        # --------------------------------------------------------------------------------------------------------------
        # Conexion de Spark
        # --------------------------------------------------------------------------------------------------------------
        log_detalle.info('Obteniendo contexto de spark...')
        conf = SparkConf() \
            .set("spark.executorEnv.PYTHONHASHSEED", "0") \
            .set("spark.sql.shuffle.partitions", "2048") \
            .set("spark.driver.maxResultSize", "30G")
        sc = SparkContext(conf=conf)
        sc.setCheckpointDir(hdfs_checkpoint_path)
        spark = SparkSession.builder.appName(app_name).getOrCreate()

        log_detalle.info('Parametros de la configuracion de spark:')
        for key, value in conf.getAll():
            log_detalle.info('> {key}: {value}'.format(key=key, value=value))

        # --------------------------------------------------------------------------------------------------------------
        # Bucle por periodos
        # --------------------------------------------------------------------------------------------------------------
        if periods_per_batch <= 0:
            periods_per_batch = num_periods
        duration = relativedelta(**{period_unit: periods_per_batch})

        for batch_min_date, batch_max_date in ut.generate_date_batches(f_min, f_max, duration):
            log_detalle.info('***** [Inicio de batch]: Fecha inicio incluida {f_min}, fecha fin excluida {f_max} *****'
                             .format(f_min=batch_min_date, f_max=batch_max_date))

            # ----------------------------------------------------------------------------------------------------------
            # Ejecucion principal
            # ----------------------------------------------------------------------------------------------------------
            log_detalle.info('Inicio de la extracción del dataset...')

            data_provider = DataProvider(spark,
                                         batch_min_date.strftime(C.PY_YYYY_MM_DD_MASK),
                                         (batch_max_date - relativedelta(days=1)).strftime(C.PY_YYYY_MM_DD_MASK),
                                         log_detalle)

            tablon_df = data_provider.transactions_df()

            log_detalle.info("Conteo del tablon final {count}".format(count=tablon_df.count()))

            log_detalle.info("Inicio del guardado del dataset. Tabla Hive: {}".format(output_tablename))

            if batch_min_date == f_min and drop_output_table:
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
    default_output_tablename = 'kschool-crypto:ks_crypto_dataset.transactions_flatten'
    default_periods_per_batch = 1
    default_drop_output_table = False

    parser = argparse.ArgumentParser(description='Creacion del tablon aplanado')
    parser.add_argument('-o', '--output_tablename',
                        default=default_output_tablename,
                        help="el nombre completo de la tabla de salida")
    parser.add_argument('-c', '--check_point',
                        help='la ruta en HDFS donde almacenar los checkpoints')
    parser.add_argument('-e', '--end_date',
                        help='la fecha de finalización del periodo de extracción (no incluida)')
    parser.add_argument('-n', '--num_periods', type=int,
                        help='número de periodos de histórico a extraer')
    parser.add_argument('-u', '--period_unit',
                        help='unidad para los periodos (years, months, days...), '
                             'basandonos en relativedelta')
    parser.add_argument('-b', '--periods_per_batch', type=int,
                        default=default_periods_per_batch,
                        help='numero de periodos de los batches. Si es menor o igual a 0, no hay batch')
    parser.add_argument('-d', '--drop_output_table', type=str2bool,
                        default=default_drop_output_table,
                        help='si la tabla de salida existe, borramos la tabla (True) '
                             'o añadimos los nuevos datos (False)?')

    return parser


class DataProvider:
    _anon_transactions_classes_df = None
    _danon_transactions_df = None
    _transactions_df = None

    def __init__(self, spark, f_min, f_max, log_detalle):
        self._spark = spark
        self._f_min = f_min
        self._f_max = f_max
        self._log_detalle = log_detalle

    def spark(self):
        return self._spark

    def f_min(self):
        return self._f_min

    def f_max(self):
        return self._f_max

    def log_detalle(self):
        return self._log_detalle

    def _get_data_from_spark(self,
                             cached_internal_attr,
                             func, func_args=(), func_kwargs={},
                             log_message=None, do_persist=False, do_checkpoint=False):
        if getattr(self, cached_internal_attr) is None:
            if log_message is not None:
                self.log_detalle().info(log_message)
            spark_df = func(*func_args, **func_kwargs)

            if do_persist or do_checkpoint:
                if do_checkpoint:
                    spark_df = spark_df.checkpoint()
                elif do_persist:
                    spark_df = spark_df.persist(StorageLevel.DISK_ONLY)
                count = spark_df.count()
                if log_message is not None:
                    self.log_detalle().info(">>>>> count: {count}".format(count=count))
            setattr(self, cached_internal_attr, spark_df)

        return getattr(self, cached_internal_attr)

    def anon_transactions_classes_df(self):
        log_message = "Extrayendo anon_transactions_classes"
        cached_internal_attr = '_anon_transactions_classes_df'
        func = extract_anon_transactions_classes
        func_kwargs = {
            'spark': self.spark()
        }

        return self._get_data_from_spark(cached_internal_attr, func, func_kwargs=func_kwargs, log_message=log_message)

    def danon_transactions_df(self):
        log_message = "Extrayendo danon_transactions"
        cached_internal_attr = '_danon_transactions_df'
        func = extract_danon_transactions
        func_kwargs = {
            'spark': self.spark(),
            'anon_transactions_classes_df': self.anon_transactions_classes_df()
        }

        return self._get_data_from_spark(cached_internal_attr, func, func_kwargs=func_kwargs, log_message=log_message)

    def transactions_df(self):
        log_message = "Extrayendo transactions"
        cached_internal_attr = '_transactions_df'
        func = extract_transactions
        func_kwargs = {
            'spark': self.spark(),
            'f_min': self.f_min(),
            'f_max': self.f_max(),
            'danon_transactions_df': self.danon_transactions_df()
        }

        return self._get_data_from_spark(cached_internal_attr, func, func_kwargs=func_kwargs, log_message=log_message)


if __name__ == "__main__":
    main()
