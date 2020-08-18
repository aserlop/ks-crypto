#!/usr/bin/env bash
# ----------------------------------------------------------------------------------------------------------------------
#                                                   PARAMETERS
# ----------------------------------------------------------------------------------------------------------------------

# names params
BUCKET_NAME=ks-crypto
CLUSTER_NAME=ks-crypto-cluster

# region params
REGION=europe-west2

# dates
DATE_END='2017-11-01'
NUM_PERIODS=22 # total 22
PERIOD_UNIT='months'


# ----------------------------------------------------------------------------------------------------------------------
#                                                   TASK
# ----------------------------------------------------------------------------------------------------------------------

# Create a dataproc
./00_start_dataproc.sh ${CLUSTER_NAME} ${REGION} ${BUCKET_NAME} && \

./01_extract_data.sh ${CLUSTER_NAME} ${REGION} ${BUCKET_NAME} ${DATE_END} ${NUM_PERIODS} ${PERIOD_UNIT}


