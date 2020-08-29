import torch
import numpy as np
import time
import random
import math


def pad_with_last_col(matrix, cols):
    out = [matrix]
    pad = [matrix[:, [-1]]] * (cols - matrix.size(1))
    out.extend(pad)
    return torch.cat(out, dim=1)


def pad_with_last_val(vect, k):
    device = 'cuda' if vect.is_cuda else 'cpu'
    pad = torch.ones(k - vect.size(0),
                     dtype=torch.long,
                     device=device) * vect[-1]
    vect = torch.cat([vect, pad])
    return vect


def sparse_prepare_tensor(tensor, torch_size, ignore_batch_dim=True):
    if ignore_batch_dim:
        tensor = sp_ignore_batch_dim(tensor)
    tensor = make_sparse_tensor(tensor,
                                tensor_type='float',
                                torch_size=torch_size)
    return tensor


def sp_ignore_batch_dim(tensor_dict):
    tensor_dict['idx'] = tensor_dict['idx'][0]
    tensor_dict['vals'] = tensor_dict['vals'][0]
    return tensor_dict


def aggregate_by_time(time_vector, time_win_aggr):
    time_vector = time_vector - time_vector.min()
    time_vector = time_vector // time_win_aggr
    return time_vector


def sort_by_time(data, time_col):
    _, sort = torch.sort(data[:, time_col])
    data = data[sort]
    return data


def print_sp_tensor(sp_tensor, size):
    print(torch.sparse.FloatTensor(sp_tensor['idx'].t(), sp_tensor['vals'], torch.Size([size, size])).to_dense())


def reset_param(t):
    stdv = 2. / math.sqrt(t.size(0))
    t.data.uniform_(-stdv, stdv)


def make_sparse_tensor(adj, tensor_type, torch_size):
    if len(torch_size) == 2:
        tensor_size = torch.Size(torch_size)
    elif len(torch_size) == 1:
        tensor_size = torch.Size(torch_size * 2)

    if tensor_type == 'float':
        test = torch.sparse.FloatTensor(adj['idx'].t(),
                                        adj['vals'].type(torch.float),
                                        tensor_size)
        return torch.sparse.FloatTensor(adj['idx'].t(),
                                        adj['vals'].type(torch.float),
                                        tensor_size)
    elif tensor_type == 'long':
        return torch.sparse.LongTensor(adj['idx'].t(),
                                       adj['vals'].type(torch.long),
                                       tensor_size)
    else:
        raise NotImplementedError('only make floats or long sparse tensors')


def sp_to_dict(sp_tensor):
    return {'idx': sp_tensor._indices().t(),
            'vals': sp_tensor._values()}


def set_seeds(rank):
    seed = int(time.time()) + rank
    np.random.seed(seed)
    random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)


def random_param_value(param, param_min, param_max, type='int'):
    if str(param) is None or str(param).lower() == 'none':
        if type == 'int':
            return random.randrange(param_min, param_max + 1)
        elif type == 'logscale':
            interval = np.logspace(np.log10(param_min), np.log10(param_max), num=100)
            return np.random.choice(interval, 1)[0]
        else:
            return random.uniform(param_min, param_max)
    else:
        return param


def load_data(file):
    with open(file) as file:
        file = file.read().splitlines()
    data = torch.tensor([[float(r) for r in row.split(',')] for row in file[1:]])
    return data


def load_data_from_tar(file, tar_archive, replace_unknow=False, starting_line=1, sep=',', type_fn=float,
                       tensor_const=torch.DoubleTensor):
    f = tar_archive.extractfile(file)
    lines = f.read()
    lines = lines.decode('utf-8')
    if replace_unknow:
        lines = lines.replace('unknow', '-1')
        lines = lines.replace('-1n', '-1')

    lines = lines.splitlines()

    data = [[type_fn(r) for r in row.split(sep)] for row in lines[starting_line:]]
    data = tensor_const(data)

    return data

# TASKER


def get_sp_adj(edges, time, weighted, time_window):
    idx = edges['idx']
    subset = idx[:, 2] <= time
    subset = subset * (idx[:, 2] > (time - time_window))
    idx = edges['idx'][subset][:, [0, 1]]
    vals = edges['vals'][subset]
    out = torch.sparse.FloatTensor(idx.t(), vals).coalesce()

    idx = out._indices().t()
    if weighted:
        vals = out._values()
    else:
        vals = torch.ones(idx.size(0), dtype=torch.long)

    return {'idx': idx, 'vals': vals}


def get_node_mask(cur_adj, num_nodes):
    mask = torch.zeros(num_nodes) - float("Inf")
    non_zero = cur_adj['idx'].unique()

    mask[non_zero] = 0

    return mask


def normalize_adj(adj, num_nodes):
    '''
    takes an adj matrix as a dict with idx and vals and normalize it by:
        - adding an identity matrix,
        - computing the degree vector
        - multiplying each element of the adj matrix (aij) by (di*dj)^-1/2
    '''
    idx = adj['idx']
    vals = adj['vals']

    sp_tensor = torch.sparse.FloatTensor(idx.t(), vals.type(torch.float), torch.Size([num_nodes, num_nodes]))

    sparse_eye = make_sparse_eye(num_nodes)
    sp_tensor = sparse_eye + sp_tensor

    idx = sp_tensor._indices()
    vals = sp_tensor._values()

    degree = torch.sparse.sum(sp_tensor, dim=1).to_dense()
    di = degree[idx[0]]
    dj = degree[idx[1]]

    vals = vals * ((di * dj) ** -0.5)

    return {'idx': idx.t(), 'vals': vals}


def make_sparse_eye(size):
    eye_idx = torch.arange(size)
    eye_idx = torch.stack([eye_idx, eye_idx], dim=1).t()
    vals = torch.ones(size)
    eye = torch.sparse.FloatTensor(eye_idx, vals, torch.Size([size, size]))
    return eye

# HYPERPARAMS

def build_random_hyper_params(args_dic):

    args_dic['learning_rate'] = random_param_value(args_dic['learning_rate'],
                                                   args_dic['learning_rate_min'],
                                                   args_dic['learning_rate_max'],
                                                   type='logscale')

    args_dic['num_hist_steps'] = random_param_value(args_dic['num_hist_steps'],
                                                    args_dic['num_hist_steps_min'],
                                                    args_dic['num_hist_steps_max'],
                                                    type='int')

    args_dic['feats_per_node'] = random_param_value(args_dic['feats_per_node'],
                                                    args_dic['feats_per_node_min'],
                                                    args_dic['feats_per_node_max'],
                                                    type='int')

    args_dic['layer_1_feats'] = random_param_value(args_dic['layer_1_feats'],
                                                   args_dic['layer_1_feats_min'],
                                                   args_dic['layer_1_feats_max'],
                                                   type='int')

    if args_dic['layer_2_feats_same_as_l1']:

        args_dic['layer_2_feats'] = args_dic['layer_1_feats']

    else:
        args_dic['layer_2_feats'] = random_param_value(args_dic['layer_2_feats'],
                                                       args_dic['layer_1_feats_min'],
                                                       args_dic['layer_1_feats_max'],
                                                       type='int')

    args_dic['lstm_l1_feats'] = random_param_value(args_dic['lstm_l1_feats'],
                                                   args_dic['lstm_l1_feats_min'],
                                                   args_dic['lstm_l1_feats_max'],
                                                   type='int')

    if args_dic['lstm_l2_feats_same_as_l1']:
        args_dic['lstm_l2_feats'] = args_dic['lstm_l1_feats']
    else:
        args_dic['lstm_l2_feats'] = random_param_value(args_dic['lstm_l2_feats'],
                                                       args_dic['lstm_l1_feats_min'],
                                                       args_dic['lstm_l1_feats_max'],
                                                       type='int')

    args_dic['cls_feats'] = random_param_value(args_dic['cls_feats'],
                                               args_dic['cls_feats_min'],
                                               args_dic['cls_feats_max'],
                                               type='int')
    return args_dic
