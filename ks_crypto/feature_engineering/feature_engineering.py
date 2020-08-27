from ks_crypto.lib import constants as C, spark_utils as su
from pyspark.ml.feature import StringIndexer
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def feature_engineering(input_df):

    output_df = \
        input_df \
        .transform(parse_class)\
        .transform(add_indexes_to_addresses)

    return output_df


def parse_class(input_df):
    return input_df.withColumn(C.CLASS, su.binarize_condition(F.col(C.CLASS) == 'illicit'))


def add_indexes_to_addresses(input_df):

    input_addresses_df = \
        input_df \
        .select(F.col(C.INPUT_ADDRESS_ID).alias(C.ADDRESS_ID))

    output_addresses_df = \
        input_df \
        .select(F.col(C.OUTPUT_ADDRESS_ID).alias(C.ADDRESS_ID))

    w_ord = Window.orderBy(C.ADDRESS_ID)

    i_address_df = \
        input_addresses_df \
        .unionByName(output_addresses_df) \
        .dropDuplicates([C.ADDRESS_ID])\
        .withColumn(C.I_ADDRESS_ID, F.row_number().over(w_ord).cast('int'))\
        .persist()

    i_address_df.count()

    input_i_address_df = \
        i_address_df\
        .select(F.col(C.ADDRESS_ID).alias(C.INPUT_ADDRESS_ID),
                F.col(C.I_ADDRESS_ID).alias(C.I_INPUT_ADDRESS_ID))

    output_i_address_df = \
        i_address_df\
        .select(F.col(C.ADDRESS_ID).alias(C.OUTPUT_ADDRESS_ID),
                F.col(C.I_ADDRESS_ID).alias(C.I_OUTPUT_ADDRESS_ID))

    output_df = \
        input_df \
        .join(input_i_address_df,
              on=[C.INPUT_ADDRESS_ID],
              how=C.LEFT) \
        .join(output_i_address_df,
              on=[C.OUTPUT_ADDRESS_ID],
              how=C.LEFT)

    return output_df

