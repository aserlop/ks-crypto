from ks_crypto.lib import constants as C


def extract_anon_transactions_classes(spark):
    output_df = \
        build_reader_anon_transactions_classes(spark) \
        .transform(transform_anon_transactions_classes)

    return output_df


def transform_anon_transactions_classes(input_df):

    output_df = \
        input_df \

    # add additional steps

    return output_df


def build_reader_anon_transactions_classes(spark):
    table = 'anon_transactions_classes'

    output_df = \
        spark.read.format("bigquery") \
        .option("table", f"{C.KS_CRYPTO_PROJ}:{C.KS_CRYPTO_DS}.{table}") \
        .load()

    return output_df
