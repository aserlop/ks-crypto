from ks_crypto.lib import constants as C


def extract_transactions(spark, f_min, f_max):
    output_df = \
        build_reader_transactions(spark, f_min, f_max) \
        .transform(transform_transactions)

    return output_df


def transform_transactions(input_df):

    output_df = \
        input_df \


    # add transformations

    return output_df


def build_reader_transactions(spark, f_min, f_max):
    table = 'transactions'

    output_df = \
        spark.read.format("bigquery") \
        .option("table", f"{C.BQ_PUB_DATA_PROJ}:{C.BITCOIN_DS}.{table}") \
        .option("filter", f"{C.BLOCK_TIMESTAMP_MONTH} >= '{f_min}' AND {C.BLOCK_TIMESTAMP_MONTH} < '{f_max}'")\
        .load()

    return output_df
