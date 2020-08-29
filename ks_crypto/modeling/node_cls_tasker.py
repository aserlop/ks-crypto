import ks_crypto.lib.modeling_utils as mu


class NodeClsTasker:
    def __init__(self, args_dic, dataset):

        self.data = dataset
        self.max_time = dataset.max_time
        self.num_classes = args_dic['num_classes']
        self.num_hist_steps = args_dic['num_hist_steps']
        self.adj_mat_time_window = args_dic['adj_mat_time_window']

        self.feats_per_node = dataset.feats_per_node
        self.nodes_labels_times = dataset.nodes_labels_times

        # funcion de preparacion para las features de los nodos (en nuestro caso devolvemos las features tal cual)
        self.get_node_feats = self.build_get_node_feats(dataset)

        # devolvemos node features tal cual
        self.prepare_node_feats = self.build_prepare_node_feats()

        self.is_static = False

    def build_get_node_feats(self, dataset):
        def get_node_feats(i, adj):
            return dataset.nodes_feats  # [i] I'm ignoring the index since the features for Elliptic are static

        return get_node_feats

    def build_prepare_node_feats(self):
        def prepare_node_feats(node_feats):
            return node_feats[0]

        return prepare_node_feats

    def get_sample(self, idx, test):
        hist_adj_list = []
        hist_ndFeats_list = []
        hist_mask_list = []

        for i in range(idx - self.num_hist_steps, idx + 1):
            # all edgess included from the beginning
            cur_adj = mu.get_sp_adj(edges=self.data.edges,
                                    time=i,
                                    weighted=True,
                                    time_window=self.adj_mat_time_window)

            node_mask = mu.get_node_mask(cur_adj, self.data.num_nodes)

            node_feats = self.get_node_feats(i, cur_adj)

            cur_adj = mu.normalize_adj(adj=cur_adj, num_nodes=self.data.num_nodes)

            hist_adj_list.append(cur_adj)
            hist_ndFeats_list.append(node_feats)
            hist_mask_list.append(node_mask)

        label_adj = self.get_node_labels(idx)

        return {'idx': idx,
                'hist_adj_list': hist_adj_list,
                'hist_ndFeats_list': hist_ndFeats_list,
                'label_sp': label_adj,
                'node_mask_list': hist_mask_list}

    def get_node_labels(self, idx):
        node_labels = self.nodes_labels_times
        subset = node_labels[:, 2] == idx
        label_idx = node_labels[subset, 0]
        label_vals = node_labels[subset, 1]

        return {'idx':  label_idx,
                'vals': label_vals}
