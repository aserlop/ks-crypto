#!/bin/bash

ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then

  ssh-keyscan -t rsa github.com > /root/.ssh/known_hosts

  git clone https://github.com/aserlop/ks-crypto.git /home/ks-crypto/

  sh /home/ks-crypto/bin/dataproc_ini/create_env.sh


  pipenv install --python 3.7 -r requirements.txt
  pipenv shell
  python -m ipykernel install --user --name=
  sudo mv /root/.local/share/jupyter/kernels/kedro/ /opt/conda/anaconda/share/jupyter/kernels/
fi