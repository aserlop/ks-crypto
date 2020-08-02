# names params
BUCKET_NAME=ks-crypto
CLUSTER_NAME=ks-crypto-cluster

# region params
REGION=europe-west2
ZONE=europe-west2-b

# cluster params
MASTER_MACHINE_TYPE=n1-standard-4
NUM_MASTERS=1
MASTER_DISK_SIZE=100GB
WORKER_MACHINE_TYPE=n1-standard-4
NUM_WORKERS=2
WORKER_DISK_SIZE=50GB

# initialization-actions
INI_ACTIONS=gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh

# gcloud init

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


# Create cluster
echo "Deploying cluster..."
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --zone=${ZONE} \
  --image-version=1.5 \
  --master-machine-type=${MASTER_MACHINE_TYPE} \
  --num-masters=${NUM_MASTERS} \
  --master-boot-disk-size=${MASTER_DISK_SIZE} \
  --worker-machine-type=${WORKER_MACHINE_TYPE} \
  --num-workers=${NUM_WORKERS} \
  --worker-boot-disk-size=${WORKER_DISK_SIZE} \
  --bucket=${BUCKET_NAME} \
  --optional-components=ANACONDA,JUPYTER \
  --enable-component-gateway \
  --metadata 'PIP_PACKAGES=google-cloud-bigquery google-cloud-storage' \
  --initialization-actions=${INI_ACTIONS}

echo "Finish!"

# Delete cluster
# gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION}
# gcloud dataproc clusters delete ks_crypto-cluster --region=europe-west2