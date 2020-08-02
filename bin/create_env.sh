#!/usr/bin/env bash

ENV_NAME=p_ks_crypto
KERNEL_NAME="k_p_ks_crypto"

conda create -n $ENV_NAME python=3.7

pushd .
cd ../
source activate $ENV_NAME
python setup.py clean
pip install .
python -m ipykernel install --user --name $ENV_NAME --display-name $KERNEL_NAME
conda deactivate
popd
