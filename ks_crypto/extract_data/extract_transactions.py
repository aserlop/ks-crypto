from pyspark.sql import functions as F
from ks_crypto.lib import constants as C


def extract_transactions(spark, f_min, f_max, danon_transactions_df=None):
    output_df = \
        build_reader_transactions(spark, f_min, f_max) \
        .transform(lambda df: transform_transactions(df, danon_transactions_df))

    return output_df


def transform_transactions(input_df, danon_transactions_df):

    general_select_list = build_general_select_list()

    output_df = \
        input_df \
        .select(*general_select_list)\
        .transform(lambda df: add_danon_data_to_transactions(df, danon_transactions_df))\
        .transform(explode_transactions_by_input_output)\
        .transform(explode_transactions_by_addresses)

    return output_df


def build_general_select_list():
    select_dic = {
        'hash':                  C.TRANSACTION_ID,
        C.BLOCK_TIMESTAMP:       C.BLOCK_TIMESTAMP,
        C.BLOCK_TIMESTAMP_MONTH: C.BLOCK_TIMESTAMP_MONTH,
        'input_count':           'total_input_count',
        'output_count':          'total_output_count',
        'input_value':           'total_input_value',
        'output_value':          'total_output_value',
        'fee':                   'total_fee',
        'inputs':                'inputs',
        'outputs':               'outputs'
    }
    return [F.col(k).alias(v) for k, v in select_dic.items()]


def add_danon_data_to_transactions(input_df, danon_transactions_df):

    if danon_transactions_df is not None:

        input_df = \
            input_df \
            .join(danon_transactions_df,
                  on=[C.TRANSACTION_ID],
                  how=C.LEFT)\
            .fillna(0, ['is_deanonymized'])

    return input_df


def explode_transactions_by_input_output(input_df):

    aux_columns = ['inputs', 'outputs']

    output_df = \
        input_df \
        .withColumn('inputs', F.explode('inputs')) \
        .withColumn('outputs', F.explode('outputs'))\
        .select('*',
                F.col('inputs.addresses').alias('input_address'),
                F.col('outputs.addresses').alias('output_address'),
                F.col('inputs.value').alias('input_value'),
                F.col('outputs.value').alias('output_value'))\
        .drop(*aux_columns)

    return output_df


def explode_transactions_by_addresses(input_df):

    output_df = \
        input_df \
        .withColumn('input_address', F.explode(C.INPUT_ADDRESS_ID))\
        .withColumn('output_address', F.explode(C.OUTPUT_ADDRESS_ID))

    return output_df


def build_reader_transactions(spark, f_min, f_max):
    table = 'transactions'

    output_df = \
        spark.read.format("bigquery") \
        .option("table", f"{C.BQ_PUB_DATA_PROJ}:{C.BITCOIN_DS}.{table}") \
        .option("filter", f"{C.BLOCK_TIMESTAMP_MONTH} >= '{f_min}' AND {C.BLOCK_TIMESTAMP_MONTH} < '{f_max}'")\
        .load()

    return output_df
