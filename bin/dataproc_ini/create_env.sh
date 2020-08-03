#!/usr/bin/env bash

ENV_NAME=p_ks_crypto
KERNEL_NAME="k_p_ks_crypto"

conda create -n $ENV_NAME python=3.7
conda activate $ENV_NAME
python /home/ks-crypto/setup.py clean
pip install /home/ks-crypto/setup.py
python -m ipykernel install --user --name $ENV_NAME
sudo mv /root/.local/share/jupyter/kernels/${ENV_NAME}/ /opt/conda/anaconda/share/jupyter/kernels/
conda deactivate
cd ~