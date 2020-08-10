#!/bin/bash

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then

  ssh-keyscan -t rsa github.com > /root/.ssh/known_hosts

  git clone --single-branch --branch develop https://github.com/aserlop/ks-crypto.git /home/ks-crypto

  sh /home/ks-crypto/bin/dataproc_ini/create_env.sh

  pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.1

fi