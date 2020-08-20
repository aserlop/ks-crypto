#!/usr/bin/env bash

ENV_NAME=p_ks_crypto

# Create environment and install setup.py (and its dependencies)
conda create -n $ENV_NAME python=3.7
conda activate $ENV_NAME
# shellcheck disable=SC2164
cd /home/ks-crypto
python setup_graphframes.py clean
pip install setup_graphframes.py
python setup.py clean
pip install setup.py

# Create zip with environment to distribute in executors
# shellcheck disable=SC2164
cd /opt/conda/anaconda/envs/
zip -rou ${ENV_NAME}.zip ${ENV_NAME}/

# Create kernel an put it in the default folder
python -m ipykernel install --user --name $ENV_NAME
sudo mv /root/.local/share/jupyter/kernels/${ENV_NAME}/ /opt/conda/anaconda/share/jupyter/kernels/
