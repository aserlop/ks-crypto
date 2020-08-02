from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_yarn_connection(app_name='ks_crypto'):

	conf = \
		SparkConf()\
		.set('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.0')

	return SparkSession.builder.config(conf=conf).appName(app_name).getOrCreate()
