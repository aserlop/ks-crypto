#!/usr/bin/env bash
# ----------------------------------------------------------------------------------------------------------------------
#                                                   PARAMETERS
# ----------------------------------------------------------------------------------------------------------------------

# names params
BUCKET_NAME=ks-crypto
CLUSTER_NAME=ks-crypto-cluster

# region params
REGION=europe-west2
ZONE=europe-west2-b

# dates
DATE_END='2017-11-01'
NUM_PERIODS=22 # total 22
PERIOD_UNIT='months'


# ----------------------------------------------------------------------------------------------------------------------
#                                                   TASK
# ----------------------------------------------------------------------------------------------------------------------

# Create a dataproc
./00_start_dataproc.sh ${CLUSTER_NAME} ${REGION} ${BUCKET_NAME} ${ZONE} && \

# Extract data
./01_extract_data.sh ${CLUSTER_NAME} ${REGION} ${BUCKET_NAME} ${DATE_END} ${NUM_PERIODS} ${PERIOD_UNIT} && \

# Filter data
./02_filter_data.sh ${CLUSTER_NAME} ${REGION} ${BUCKET_NAME} ${DATE_END} ${NUM_PERIODS} ${PERIOD_UNIT} && \

# Feature engineering
./03_feature_engineering.sh ${CLUSTER_NAME} ${REGION} ${BUCKET_NAME} ${DATE_END} ${NUM_PERIODS} ${PERIOD_UNIT} && \

./04_modeling_sh ${CLUSTER_NAME} ${REGION} ${BUCKET_NAME} ${DATE_END} ${NUM_PERIODS} ${PERIOD_UNIT} && \

# Close dataproc
./05_close_dataproc.sh ${CLUSTER_NAME} ${REGION}

