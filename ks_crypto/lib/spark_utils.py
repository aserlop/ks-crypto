from pyspark.sql import functions as F


def export_to_big_query(df, full_tablename, mode='overwrite', partition_by=None):

    df.write.format('bigquery')\
    .option('table', full_tablename) \
    .option('partitionField', partition_by)\
    .mode(mode) \
    .save()


def binarize_condition(cond):
    return F.when(cond, 1).otherwise(0)

def date_diff_sec(colname_1, colname_2):
    return (F.col(colname_1).cast('long') - F.col(colname_2).cast('long')).cast('int')
