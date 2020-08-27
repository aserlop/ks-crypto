from ks_crypto.lib import constants as C, utils as ut
from graphframes import GraphFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def filter_data(input_df, generate_periods=False):

    output_df = \
        input_df \
        .transform(lambda df: add_period_block_timestamp(df, generate_periods))\
        .filter(F.col(C.PERIOD).isNotNull())\
        .transform(lambda df: add_is_inside_danon_community(df))\
        .filter(F.col('is_inside_danon_community') == 1)\
        .drop(*['is_inside_danon_community'])

    return output_df


# ------------------------------------------ COMUNITIES ----------------------------------------------------------------

def add_is_inside_danon_community(input_df):

    vertex_df = transform_to_vertex(input_df)
    edges_df = transform_to_edges(input_df)

    graph = GraphFrame(vertex_df, edges_df)

    w = Window.partitionBy('component')

    communities_df = \
        graph.connectedComponents()\
        .withColumn('is_inside_danon_community', F.max('is_deanonymized').over(w)) \
        .filter(F.col('is_inside_danon_community') == 1) \
        .select(C.ID,
                'is_inside_danon_community') \
        .persist()

    communities_df.count()

    communities_input_df = \
        communities_df\
        .select(F.col(C.ID).alias(C.INPUT_ADDRESS_ID),
                F.lit(1).alias('is_inside_danon_community_input'))

    communities_output_df = \
        communities_df\
        .select(F.col(C.ID).alias(C.OUTPUT_ADDRESS_ID),
                F.lit(1).alias('is_inside_danon_community_output'))

    aux_columns = ['is_inside_danon_community_output', 'is_inside_danon_community_input']

    output_df = \
        input_df\
        .join(communities_input_df,
              on=[C.INPUT_ADDRESS_ID],
              how=C.LEFT) \
        .join(communities_output_df,
              on=[C.OUTPUT_ADDRESS_ID],
              how=C.LEFT) \
        .withColumn('is_inside_danon_community',
                    F.greatest('is_inside_danon_community_output', 'is_inside_danon_community_input'))\
        .drop(*aux_columns)

    return output_df


def transform_to_vertex(input_df):

    input_addresses_df = \
        input_df \
        .select(F.col(C.INPUT_ADDRESS_ID).alias(C.ID),
                'is_deanonymized')

    output_addresses_df = \
        input_df \
        .select(F.col(C.OUTPUT_ADDRESS_ID).alias(C.ID),
                'is_deanonymized')

    output_df = \
        input_addresses_df \
        .unionByName(output_addresses_df) \
        .dropDuplicates([C.ID]) \
        .repartition(256) \
        .persist()

    output_df.count()

    return output_df


def transform_to_edges(input_df):
    output_df = \
        input_df \
        .select(F.col(C.INPUT_ADDRESS_ID).alias(C.SRC),
                F.col(C.OUTPUT_ADDRESS_ID).alias(C.DST),
                'is_deanonymized')\
        .repartition(256)\
        .persist()

    output_df.count()

    return output_df


# ------------------------------------------ PERIOD --------------------------------------------------------------------


def add_period_block_timestamp(input_df, generate_periods):

    if generate_periods is not None:
        period_dic = build_period_block_timestamp_dic(input_df, C.OLD_NUM_HOURS_FROM_EVENT, C.NEW_NUM_HOURS_FROM_EVENT)

    else:
        period_dic = C.PERIOD_DIC

    period_fun = build_period_fun_from_dic(period_dic, C.BLOCK_TIMESTAMP)

    return input_df.withColumn(C.PERIOD, period_fun)


def build_period_block_timestamp_dic(input_df, old_num_hours, new_num_hours):
    w_ord = Window.orderBy(C.BLOCK_TIMESTAMP)
    w_id = Window.partitionBy(C.BLOCK_TIMESTAMP)

    diff_hours_lag_fun = (
            (F.col(C.BLOCK_TIMESTAMP).cast('long') -
             F.col(f'lag_{C.BLOCK_TIMESTAMP}').cast('long')) / 3600).cast('int')
    is_more_hours_lag_cond = \
        (diff_hours_lag_fun > old_num_hours) | (F.col(f'lag_{C.BLOCK_TIMESTAMP}').isNull())

    final_cols_dic = {
        'period': F.row_number().over(w_ord),
        'f_min_period': F.col(C.BLOCK_TIMESTAMP).cast('string'),
        'f_max_period': (F.col(C.BLOCK_TIMESTAMP) + F.expr(f'INTERVAL {new_num_hours} HOURS')).cast('string')
    }

    final_cols_list = [v.alias(k) for k, v in final_cols_dic.items()]

    period_df = \
        input_df \
        .filter(F.col('is_deanonymized') == 1) \
        .withColumn(f'lag_{C.BLOCK_TIMESTAMP}', F.lag(C.BLOCK_TIMESTAMP).over(w_ord)) \
        .withColumn('is_first_in_period', F.when(is_more_hours_lag_cond, 1).otherwise(0)) \
        .withColumn('is_first_in_period', F.max('is_first_in_period').over(w_id)) \
        .dropDuplicates([C.BLOCK_TIMESTAMP, 'is_first_in_period']) \
        .filter(F.col('is_first_in_period') == 1) \
        .select(*final_cols_list) \
        .toPandas()

    dic = {int(period_df['period'][i]): [period_df['f_min_period'][i], period_df['f_max_period'][i]]
           for i in period_df.index.values.tolist()}

    return dic


def build_period_fun_from_dic(dic, colname):
    stacked_cond = None
    for k, v in dic.items():
        cond = (F.col(colname) >= v[0]) & (F.col(colname) <= v[1])
        stacked_cond = stacked_cond.when(cond, k) if stacked_cond is not None else F.when(cond, k)

    return stacked_cond
