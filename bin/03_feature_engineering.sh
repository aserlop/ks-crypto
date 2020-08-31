#!/usr/bin/env bash
# ----------------------------------------------------------------------------------------------------------------------
#                                                   PARAMETERS
# ----------------------------------------------------------------------------------------------------------------------

CLUSTER_NAME=$1
REGION=$2
BUCKET_NAME=$3

DATE_END=$4
NUM_PERIODS=$5
PERIOD_UNIT=$6

# Generic Config Spark
APP_NAME="ks_crypto_ft_eng"
HDFS_FULL_PATH="/"
HDFS_FULL_PATH_CHECKPOINT=${HDFS_FULL_PATH}"temp/"

# Environment config
ENV_NAME=p_ks_crypto
DENV_FULL_PATH=/opt/conda/anaconda/envs/${ENV_NAME}.zip#DENV # Add #DENV at the end
PYTHON_DENV_REL_PATH=./DENV/${ENV_NAME}/bin/python
BQ_CONNECTOR_URI='gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
GF_PACKAGE='gs://ks-crypto/graphframes-0.8.0-spark3.0-s_2.12.jar'
# GF_PACKAGE='graphframes:graphframes:0.8.0-spark3.0-s_2.12'

# Task config
TASK_MODULE_REL_PATH="../ks_crypto/feature_engineering/task.py"
DROP_OUTPUT_TABLE=1
BQ_INPUT_FULL_TABLENAME="kschool-crypto:ks_crypto_dataset.transactions_flatten_filt"
T_BQ_OUTPUT_FULL_TABLENAME="kschool-crypto:ks_crypto_dataset.transactions_ft"
N_BQ_OUTPUT_FULL_TABLENAME="kschool-crypto:ks_crypto_dataset.nodes_ft"

# ----------------------------------------------------------------------------------------------------------------------
#                                                   TASK
# ----------------------------------------------------------------------------------------------------------------------

## Launch Task

gcloud dataproc jobs submit pyspark \
--cluster=${CLUSTER_NAME} \
--region=${REGION} \
--bucket=${BUCKET_NAME} \
--jars="${BQ_CONNECTOR_URI},${GF_PACKAGE}" \
--properties="
spark.master=yarn,
spark.submit.deployMode=cluster,
spark.app.name=${APP_NAME},
yarn:spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYTHON_DENV_REL_PATH},
yarn:spark.yarn.maxAppAttempts=1,
yarn:spark.yarn.dist.archives=${DENV_FULL_PATH}" \
${TASK_MODULE_REL_PATH} \
-- \
--end_date "${DATE_END}" \
--input_tablename "${BQ_INPUT_FULL_TABLENAME}" \
--t_output_tablename "${T_BQ_OUTPUT_FULL_TABLENAME}" \
--n_output_tablename "${N_BQ_OUTPUT_FULL_TABLENAME}" \
--check_point ${HDFS_FULL_PATH_CHECKPOINT} \
--temp_bucket_name ${BUCKET_NAME} \
--num_periods ${NUM_PERIODS} \
--period_unit ${PERIOD_UNIT} \
--drop_output_table ${DROP_OUTPUT_TABLE}