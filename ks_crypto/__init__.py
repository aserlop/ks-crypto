import argparse
from ks_crypto.lib import utils as ut
from pyspark.sql.dataframe import DataFrame


DataFrame.transform = ut.transform


def str2bool(v):
    if isinstance(v, bool):
        return v

    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True

    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False

    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')
