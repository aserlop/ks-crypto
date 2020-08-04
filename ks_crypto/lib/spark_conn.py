from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_yarn_connection(app_name='ks_crypto', env_name='p_ks_crypto'):

	conf = \
		SparkConf() \
		.setMaster('yarn') \
		.set("spark.yarn.dist.archives", f"/opt/conda/anaconda/envs/{env_name}.zip#DENV") \
		.set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", f"./DENV/{env_name}/bin/python") \
		.set('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.17.0')

	return SparkSession.builder.config(conf=conf).appName(app_name).getOrCreate()
