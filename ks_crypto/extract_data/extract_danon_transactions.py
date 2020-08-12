from ks_crypto.lib import constants as C


def extract_danon_transactions(spark):
    output_df = \
        build_reader_danon_transactions(spark) \
        .transform(transform_danon_transactions)

    return output_df


def transform_danon_transactions(input_df):

    output_df = \
        input_df \
        .select(C.TRANSACTION_ID)

    # add transformations

    return output_df


def build_reader_danon_transactions(spark):
    table = 'danon_transactions'

    output_df = \
        spark.read.format("bigquery") \
        .option("table", f"{C.KS_CRYPTO_PROJ}:{C.KS_CRYPTO_DS}.{table}") \
        .load()

    return output_df
