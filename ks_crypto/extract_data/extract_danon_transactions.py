from ks_crypto.lib import constants as C
from pyspark.sql import functions as F


def extract_danon_transactions(spark, anon_transactions_classes_df=None):
    output_df = \
        build_reader_danon_transactions(spark) \
        .transform(lambda df: transform_danon_transactions(df, anon_transactions_classes_df))

    return output_df


def transform_danon_transactions(input_df, anon_transactions_classes_df):

    aux_columns = ['anon_transaction_hash']

    output_df = \
        input_df \
        .withColumn('is_deanonymized', F.lit(1)) \
        .transform(lambda df: add_classes_to_danon_transactions(df, anon_transactions_classes_df))\
        .drop(*aux_columns)

    return output_df


def add_classes_to_danon_transactions(input_df, anon_transactions_classes_df):

    if anon_transactions_classes_df is not None:

        input_df = \
            input_df\
            .join(anon_transactions_classes_df,
                  on=['anon_transaction_hash'],
                  how=C.LEFT)

    return input_df


def build_reader_danon_transactions(spark):
    table = 'danon_transactions'

    output_df = \
        spark.read.format("bigquery") \
        .option("table", f"{C.KS_CRYPTO_PROJ}:{C.KS_CRYPTO_DS}.{table}") \
        .load()

    return output_df
