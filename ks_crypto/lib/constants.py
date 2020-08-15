# ======================================================================================================================
#                                                    CONSTANTS
# ======================================================================================================================

# ----------------------------------------------------------------------------------------------------------------------
#                                                     GENERAL
# ----------------------------------------------------------------------------------------------------------------------

KS_CRYPTO_PROJ = 'kschool-crypto'
KS_CRYPTO_DS = 'ks_crypto_dataset'

BQ_PUB_DATA_PROJ = 'bigquery-public-data'
BITCOIN_DS = 'crypto_bitcoin'

F_MIN = '2016-01-01'  # incluida
F_MAX = '2017-10-01'  # incluida

PY_YYYYMMDD_MASK = '%Y%m%d'
PY_YYYY_MM_DD_MASK = '%Y-%m-%d'
SPARK_YYYYMMDD_MASK = 'yyyyMMdd'
SPARK_YYYY_MM_DD_MASK = 'yyyy-MM-dd'

BLOCK_TIMESTAMP = 'block_timestamp'
BLOCK_TIMESTAMP_MONTH = 'block_timestamp_month'

TRANSACTION_ID = 'transaction_hash'
ADDRESS_ID = 'address'
INPUT_ADDRESS_ID = 'input_address'
OUTPUT_ADDRESS_ID = 'output_address'

ID = 'id'
SRC = 'src'
DST = 'dst'

# tipos de join
LEFT_OUTER = 'left_outer'
RIGHT_OUTER = 'right_outer'
LEFT_ANTI = 'left_anti'
INNER = 'inner'
OUTER = 'outer'
LEFT = 'left'
FULL = 'full'

SEED = 1234
