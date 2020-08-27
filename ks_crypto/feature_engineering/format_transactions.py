from ks_crypto.lib import constants as C, spark_utils as su
from graphframes import GraphFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transactions_aggregation_by_period(input_df):

    group_columns = [
        C.I_INPUT_ADDRESS_ID,  C.INPUT_ADDRESS_ID,
        C.I_OUTPUT_ADDRESS_ID, C.OUTPUT_ADDRESS_ID,
        C.PERIOD]

    w_ord = Window.partitionBy(*group_columns).orderBy(C.BLOCK_TIMESTAMP)

    agg_fun_list = \
        build_transactions_agg_fun_list() + \
        [F.max(C.CLASS).alias(C.CLASS), F.min(C.BLOCK_TIMESTAMP_MONTH).alias(C.BLOCK_TIMESTAMP_MONTH)]

    output_df = \
        input_df \
        .withColumn('lit', F.lit(1))\
        .withColumn(f'lag_{C.BLOCK_TIMESTAMP}', F.lag(C.BLOCK_TIMESTAMP).over(w_ord))\
        .withColumn(f'dif_lag_{C.BLOCK_TIMESTAMP}', su.date_diff_sec(C.BLOCK_TIMESTAMP, f'lag_{C.BLOCK_TIMESTAMP}')) \
        .groupBy(*group_columns)\
        .agg(*agg_fun_list)\
        .withColumn(f'range_{C.BLOCK_TIMESTAMP}',
                    su.date_diff_sec(f'max_{C.BLOCK_TIMESTAMP}', f'min_{C.BLOCK_TIMESTAMP}'))\
        .drop(f'max_{C.BLOCK_TIMESTAMP}', f'min_{C.BLOCK_TIMESTAMP}')

    return output_df


def build_transactions_agg_fun_list():

    continuous_colname_list = [
        'total_input_count', 'total_output_count', 'total_input_value', 'total_output_value', 'total_fee',
        'input_value', 'output_value'
    ]

    dic_fun = {
        F.sum:           continuous_colname_list,
        F.mean:          continuous_colname_list + [f'dif_lag_{C.BLOCK_TIMESTAMP}'],
        F.min:           continuous_colname_list + [C.BLOCK_TIMESTAMP],
        F.max:           continuous_colname_list + [C.BLOCK_TIMESTAMP],
        F.stddev:        continuous_colname_list,
        F.skewness:      continuous_colname_list,
        F.kurtosis:      continuous_colname_list,
        F.count:         ['lit'],
        F.countDistinct: [C.BLOCK_TIMESTAMP],
    }
    return [k(c).alias(k.__name__ + '_' + c) for k, v in dic_fun.items() for c in v]
