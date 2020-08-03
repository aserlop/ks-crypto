#!/usr/bin/env bash

ENV_NAME=p_ks_crypto
KERNEL_NAME="k_p_ks_crypto"

conda create -n $ENV_NAME python=3.7
conda activate $ENV_NAME
cd /home/ks-crypto
python setup.py clean
pip install .
python -m ipykernel install --user --name $ENV_NAME
sudo mv /root/.local/share/jupyter/kernels/${ENV_NAME}/ /opt/conda/anaconda/share/jupyter/kernels/
