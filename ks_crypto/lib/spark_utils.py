

def export_to_big_query(df, full_tablename, mode='overwrite', partition_by=None):

    df.write.format('bigquery')\
    .option('table', full_tablename) \
    .option('partitionField', partition_by)\
    .mode(mode) \
    .save()
