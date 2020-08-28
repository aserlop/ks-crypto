#!/usr/bin/env bash

CLUSTER_NAME=$1
REGION=$2

gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION}