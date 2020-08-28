#!/usr/bin/env bash

CLUSTER_NAME=$1
REGION=$2
BUCKET_NAME=$3
ZONE=$4

echo "Enabling services config..."
gcloud services enable \
  dataproc.googleapis.com \
  compute.googleapis.com \
  storage-component.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com

echo "Uploading files... "
gsutil cp -r ./dataproc_ini/* gs://${BUCKET_NAME}/ks-crypto/dataproc_ini/

echo "Deploying cluster..."
sh ./dataproc_ini/deploy_cluster.sh ${CLUSTER_NAME} ${REGION} ${ZONE} ${BUCKET_NAME}

echo "Finish!"

# BACKUP COMMANDS

# Config session
# gcloud init

# Create bucket
# echo "Creating new bucket..."
# gsutil mb -c standard -l ${REGION} gs://${BUCKET_NAME}

# Delete cluster
# gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION}
# gcloud dataproc clusters delete ks_crypto-cluster --region=europe-west2