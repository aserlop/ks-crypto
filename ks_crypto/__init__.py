from pyspark.sql.dataframe import DataFrame
from ks_crypto.lib import utils as ut

DataFrame.transform = ut.transform
