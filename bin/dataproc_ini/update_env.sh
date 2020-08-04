#!/usr/bin/env bash

ENV_NAME=p_ks_crypto

# Update environment installing setup.py again
conda activate $ENV_NAME
# shellcheck disable=SC2164
cd /home/ks-crypto
python setup.py clean
pip install .

# Create zip with environment to distribute in executors
# shellcheck disable=SC2164
cd /opt/conda/anaconda/envs/
zip -rou ${ENV_NAME}.zip ${ENV_NAME}/
