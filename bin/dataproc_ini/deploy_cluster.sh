#!/usr/bin/env bash

CLUSTER_NAME=$1
REGION=$2
ZONE=$3
BUCKET_NAME=$4

# initialization-actions
INI_BASE_GS_PATH=gs://${BUCKET_NAME}/ks-crypto/dataproc_ini

# cluster params
MASTER_MACHINE_TYPE=n1-standard-4
NUM_MASTERS=1
MASTER_DISK_SIZE=100GB
WORKER_MACHINE_TYPE=n1-standard-4
NUM_WORKERS=20
NUM_CORES_PER_WORKER=2
WORKER_DISK_SIZE=50GB

# Create cluster
gcloud dataproc clusters create \
  "${CLUSTER_NAME}" \
  --region="${REGION}" \
  --zone="${ZONE}" \
  --image-version=preview \
  --master-machine-type=${MASTER_MACHINE_TYPE} \
  --num-masters=${NUM_MASTERS} \
  --master-boot-disk-size=${MASTER_DISK_SIZE} \
  --worker-machine-type=${WORKER_MACHINE_TYPE} \
  --num-workers=${NUM_WORKERS} \
  --worker-boot-disk-size=${WORKER_DISK_SIZE} \
  --bucket="${BUCKET_NAME}" \
  --optional-components=ANACONDA,JUPYTER \
  --enable-component-gateway \
  --metadata 'PIP_PACKAGES=google-cloud-bigquery google-cloud-storage' \
  --initialization-actions="${INI_BASE_GS_PATH}/pip_install.sh,${INI_BASE_GS_PATH}/bootstrap.sh" \
  --properties="spark:spark.executor.cores=${NUM_CORES_PER_WORKER}"

