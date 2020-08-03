#!/usr/bin/env bash

ENV_NAME=p_ks_crypto

pushd .
cd ../../
source activate $ENV_NAME
python setup.py clean
pip install .
conda deactivate
popd