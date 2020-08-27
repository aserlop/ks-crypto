from ks_crypto.lib import constants as C
from pyspark.sql import functions as F


def transform_to_node_format(input_df):

    output_df = \
        input_df\
        .transform(combine_input_output_nodes)\
        .transform(nodes_aggregation_by_period)

    return output_df


def combine_input_output_nodes(input_df):

    colnames_to_rename = [c for c, t in input_df.dtypes
                          if t not in ['string', 'date'] and 'address' not in c and c not in [C.PERIOD, C.CLASS]]

    input_renamed_col_list = \
        [F.col(c).alias('in_' + c) for c in colnames_to_rename]
    output_renamed_col_list = \
        [F.col(c).alias('out_' + c) for c in colnames_to_rename]

    input_nodes_df = \
        input_df \
        .select(F.col(C.I_INPUT_ADDRESS_ID).alias(C.I_ADDRESS_ID),
                C.PERIOD,
                C.BLOCK_TIMESTAMP_MONTH,
                *input_renamed_col_list)

    output_nodes_df = \
        input_df \
        .select(F.col(C.I_OUTPUT_ADDRESS_ID).alias(C.I_ADDRESS_ID),
                C.PERIOD,
                C.BLOCK_TIMESTAMP_MONTH,
                *output_renamed_col_list)

    not_in_input_col_list = [F.lit(None).alias(c) for c in output_nodes_df.columns if c not in input_nodes_df.columns]
    not_in_output_col_list = [F.lit(None).alias(c) for c in input_nodes_df.columns if c not in output_nodes_df.columns]

    nodes_df = \
        input_nodes_df \
        .select('*', *not_in_input_col_list) \
        .unionByName(output_nodes_df.select('*', *not_in_output_col_list))

    return nodes_df


def nodes_aggregation_by_period(input_df):

    output_df = \
        input_df \
        .groupBy('i_address', 'period') \
        .agg(*build_nodes_agg_fun_list(input_df.dtypes),
             F.min(C.BLOCK_TIMESTAMP_MONTH).alias(C.BLOCK_TIMESTAMP_MONTH)) \
        .fillna(0)

    return output_df


def build_nodes_agg_fun_list(dtypes):

    agg_colname_list = [c for c, t in dtypes if t not in ['string', 'date'] and 'address' not in c and c != C.PERIOD]

    sum_colname_list = [c for c in agg_colname_list if c.startswith(('in_sum_', 'out_sum_'))]
    mean_colname_list = [c for c in agg_colname_list if c.startswith(('in_mean_', 'out_mean'))]
    min_max_colname_list = [c for c in agg_colname_list if c.startswith(('in_min_', 'in_max_', 'out_min_', 'out_max_'))]
    std_colname_list = [c for c in agg_colname_list if c.startswith(('in_stddev_', 'out_stddev_'))]
    count_colname_list = ['in_count_lit', 'out_count_lit']
    count_dist_colname_list = ['in_countDistinct_block_timestamp', 'out_countDistinct_block_timestamp']
    range_colname_list = ['in_range_block_timestamp', 'out_range_block_timestamp']

    dic_fun = {
      F.sum:           sum_colname_list,
      F.mean:          sum_colname_list + mean_colname_list + range_colname_list + std_colname_list,
      F.min:           sum_colname_list + min_max_colname_list,
      F.max:           sum_colname_list + min_max_colname_list,
      F.stddev:        sum_colname_list,
      F.count:         count_colname_list + count_dist_colname_list,
      F.countDistinct: count_colname_list
    }
    return [k(c).alias(k.__name__ + '_' + c) for k, v in dic_fun.items() for c in v]


