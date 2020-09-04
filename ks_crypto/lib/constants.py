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

F_MIN = '2016-01-01'  # included
F_MAX = '2017-11-01'  # not included

PY_YYYYMMDD_MASK = '%Y%m%d'
PY_YYYY_MM_DD_MASK = '%Y-%m-%d'
SPARK_YYYYMMDD_MASK = 'yyyyMMdd'
SPARK_YYYY_MM_DD_MASK = 'yyyy-MM-dd'

BLOCK_TIMESTAMP = 'block_timestamp'
BLOCK_TIMESTAMP_MONTH = 'block_timestamp_month'
PERIOD = 'period'

OLD_NUM_HOURS_FROM_EVENT = 3
NEW_NUM_HOURS_FROM_EVENT = 3  # max to avoid overlap is 277

TRANSACTION_ID = 'transaction_hash'
ADDRESS_ID = 'address'
I_ADDRESS_ID = 'i_address'
INPUT_ADDRESS_ID = 'input_address'
I_INPUT_ADDRESS_ID = 'i_input_address'
OUTPUT_ADDRESS_ID = 'output_address'
I_OUTPUT_ADDRESS_ID = 'i_output_address'

CLASS = 'class'

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

# ----------------------------------------------------------------------------------------------------------------------
#                                                     PERIODS
# ----------------------------------------------------------------------------------------------------------------------

PERIOD_DIC = {
    1:  ['2016-01-01 02:50:56', '2016-01-01 08:50:56'],
    2:  ['2016-01-13 23:40:57', '2016-01-14 05:40:57'],
    3:  ['2016-01-27 04:01:32', '2016-01-27 10:01:32'],
    4:  ['2016-02-07 17:04:34', '2016-02-07 23:04:34'],
    5:  ['2016-02-20 05:43:02', '2016-02-20 11:43:02'],
    6:  ['2016-03-05 14:13:59', '2016-03-05 20:13:59'],
    7:  ['2016-03-19 03:05:12', '2016-03-19 09:05:12'],
    8:  ['2016-04-01 21:14:12', '2016-04-02 03:14:12'],
    9:  ['2016-04-14 22:53:05', '2016-04-15 04:53:05'],
    10: ['2016-04-29 01:55:24', '2016-04-29 07:55:24'],
    11: ['2016-05-11 23:10:38', '2016-05-12 05:10:38'],
    12: ['2016-05-25 15:34:08', '2016-05-25 21:34:08'],
    13: ['2016-06-08 18:08:38', '2016-06-09 00:08:38'],
    14: ['2016-06-21 22:20:59', '2016-06-22 04:20:59'],
    15: ['2016-07-05 17:19:48', '2016-07-05 23:19:48'],
    16: ['2016-07-19 17:27:38', '2016-07-19 23:27:38'],
    17: ['2016-08-03 10:01:52', '2016-08-03 16:01:52'],
    18: ['2016-08-16 13:52:56', '2016-08-16 19:52:56'],
    19: ['2016-08-30 06:26:15', '2016-08-30 12:26:15'],
    20: ['2016-09-12 19:15:16', '2016-09-13 01:15:16'],
    21: ['2016-09-25 21:50:08', '2016-09-26 03:50:08'],
    22: ['2016-10-09 00:36:09', '2016-10-09 06:36:09'],
    23: ['2016-10-23 10:58:25', '2016-10-23 16:58:25'],
    24: ['2016-11-06 07:34:13', '2016-11-06 13:34:13'],
    25: ['2016-11-18 23:59:04', '2016-11-19 05:59:04'],
    26: ['2016-12-02 19:42:18', '2016-12-03 01:42:18'],
    27: ['2016-12-15 20:26:20', '2016-12-16 02:26:20'],
    28: ['2016-12-29 09:37:43', '2016-12-29 15:37:43'],
    29: ['2017-01-11 12:49:44', '2017-01-11 18:49:44'],
    30: ['2017-01-23 15:14:17', '2017-01-23 21:14:17'],
    31: ['2017-02-05 13:26:08', '2017-02-05 19:26:08'],
    32: ['2017-02-19 01:06:55', '2017-02-19 07:06:55'],
    33: ['2017-03-04 11:05:28', '2017-03-04 17:05:28'],
    34: ['2017-03-18 00:21:41', '2017-03-18 06:21:41'],
    35: ['2017-03-31 07:38:56', '2017-03-31 13:38:56'],
    36: ['2017-04-13 20:45:41', '2017-04-14 02:45:41'],
    37: ['2017-04-27 20:05:46', '2017-04-28 02:05:46'],
    38: ['2017-05-10 20:00:50', '2017-05-11 02:00:50'],
    39: ['2017-05-23 22:00:57', '2017-05-24 04:00:57'],
    40: ['2017-06-05 05:37:21', '2017-06-05 11:37:21'],
    41: ['2017-06-18 16:13:50', '2017-06-18 22:13:50'],
    42: ['2017-07-02 13:09:19', '2017-07-02 19:09:19'],
    43: ['2017-07-15 03:48:27', '2017-07-15 09:48:27'],
    44: ['2017-07-28 01:01:49', '2017-07-28 07:01:49'],
    45: ['2017-08-10 05:38:54', '2017-08-10 11:38:54'],
    46: ['2017-08-25 00:54:59', '2017-08-25 06:54:59'],
    47: ['2017-09-07 03:20:23', '2017-09-07 09:20:23'],
    48: ['2017-09-19 00:54:47', '2017-09-19 06:54:47'],
    49: ['2017-10-02 16:58:52',	'2017-10-02 19:57:31']}

# ----------------------------------------------------------------------------------------------------------------------
#                                                     EVOLVE GCN
# ----------------------------------------------------------------------------------------------------------------------

EVOLVE_GCN_MODEL_PARAMS_DIC = {
    'device': 'cpu',
    'use_cuda': True,
    'use_logfile': True,

    'class_weights': [0.25, 0.75],
    'use_2_hot_node_feats': False,
    'use_1_hot_node_feats': False,
    'save_node_embeddings': True,

    'train_proportion': 0.65,
    'dev_proportion': 0.1,
    'num_epochs': 1000,
    'steps_accum_gradients': 1,
    'learning_rate': 0.001,
    'learning_rate_min': 0.001,
    'learning_rate_max': 0.02,
    'negative_mult_training': 20,
    'negative_mult_test': 100,
    'smart_neg_sampling': False,
    'seed': SEED,
    'target_measure': 'F1',  # measure to define the best epoch F1, Precision, Recall, MRR, MAP
    'target_class': 1,  # Target class to get the measure to define the best epoch (all, 0, 1)
    'early_stop_patience': 100,

    'eval_after_epochs': 5,
    'adj_mat_time_window': 1,  # Time window to create the adj matrix for each timestep.
    'adj_mat_time_window_min': 1,
    'adj_mat_time_window_max': 10,
    'num_hist_steps': 5,  # number of previous steps used for prediction
    'num_hist_steps_min': 3,  # only used if num_hist_steps: None
    'num_hist_steps_max': 10,  # only used if num_hist_steps: None

    'feats_per_node': 50,
    'feats_per_node_min': 30,
    'feats_per_node_max': 312,
    'layer_1_feats': 76,
    'layer_1_feats_min': 30,
    'layer_1_feats_max': 500,
    'layer_2_feats': None,
    'layer_2_feats_same_as_l1': True,
    'k_top_grcu': 200,
    'num_layers': 2,
    'lstm_l1_layers': 1,
    'lstm_l1_feats': 125,
    'lstm_l1_feats_min': 50,
    'lstm_l1_feats_max': 500,
    'lstm_l2_layers': 1,
    'lstm_l2_feats': 400,
    'lstm_l2_feats_same_as_l1': True,
    'cls_feats': 510,
    'cls_feats_min': 100,
    'cls_feats_max': 700
}

# ----------------------------------------------------------------------------------------------------------------------
#                                                     T-GCN
# ----------------------------------------------------------------------------------------------------------------------





