#!/usr/bin/env bash

ENV_NAME=p_ks_crypto
KERNEL_NAME="k_p_ks_crypto"

conda create -n $ENV_NAME python=3.7
conda activate $ENV_NAME
python /home/ks-crypto/setup.py clean
sudo pip install .
python -m ipykernel install --user --name $ENV_NAME --display-name $KERNEL_NAME
sudo mv /root/.local/share/jupyter/kernels/${KERNEL_NAME}/ /opt/conda/anaconda/share/jupyter/kernels/
sudo conda deactivate