import torch

from pyspark.sql import functions as F


class TorchGraphDataProvider:
    def __init__(self, nodes_df, transactions_df):

        self.nodes_labels_times = self.load_node_labels(nodes_df)

        self.edges = self.load_transactions(transactions_df)

        self.nodes, self.nodes_feats = self.load_node_feats(nodes_df)

        self.num_nodes = len(self.nodes)
        self.feats_per_node = self.nodes.size(1) - 1

        self.max_time = self.nodes_labels_times[:, 2].max()
        self.min_time = self.nodes_labels_times[:, 2].min()

    def load_node_feats(self, nodes_df):

        casted_col_list = [F.col(col).cast('float') for col in nodes_df.columns]

        nodes_df = nodes_df.select(*casted_col_list)

        nodes = torch.tensor(nodes_df.toPandas().values)
        nodes_feats = torch.tensor(nodes_df.drop('i_address').toPandas().values)

        return nodes, nodes_feats

    def load_node_labels(self, nodes_df):

        nodes_labels_times_df = \
            nodes_df \
            .select(F.col('i_address').cast('float'),
                    F.col('class'),
                    F.col('period').cast('float')) \

        nodes_labels_times = torch.tensor(nodes_labels_times_df.toPandas().values)

        return nodes_labels_times

    def load_transactions(self, transactions_df):

        transactions_df = \
            transactions_df \
            .select(F.col('i_input_address').cast('float'),
                    F.col('i_output_address').cast('float'),
                    F.col('period').cast('float')) \
            .dropDuplicates()

        transactions = torch.tensor(transactions_df.toPandas().values)

        return {'idx': transactions, 'vals': torch.ones(transactions.size(0))}


