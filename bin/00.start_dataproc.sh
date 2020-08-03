# names params
BUCKET_NAME=ks-crypto
CLUSTER_NAME=ks-crypto-cluster

# region params
REGION=europe-west2
ZONE=europe-west2-b


echo "Enabling services config..."
gcloud services enable \
  dataproc.googleapis.com \
  compute.googleapis.com \
  storage-component.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com

# Create bucket
# echo "Creating new bucket..."
# gsutil mb -c standard -l ${REGION} gs://${BUCKET_NAME}

echo "Uploading files "
gsutil cp -r ./dataproc_ini/* gs://${BUCKET_NAME}/ks-crypto/dataproc_ini/

echo "Deploying cluster..."
sh ./dataproc_ini/deploy_cluster.sh ${CLUSTER_NAME} ${REGION} ${ZONE} ${BUCKET_NAME}

echo "Finish!"

# Config session
# gcloud init

# Delete cluster
# gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION}
# gcloud dataproc clusters delete ks_crypto-cluster --region=europe-west2