import torch
from ks_crypto.lib import modeling_utils as mu, spark_utils as su, constants as C
import ks_crypto.modeling.model_logger as logger
import time
import pandas as pd
import numpy as np
import datetime
import os


class Trainer:
    def __init__(self, args_dic, splitter, gcn, classifier, comp_loss, dataset, num_classes, logging, spark,
                 drop_output_table):
        self.args_dic = args_dic
        self.splitter = splitter
        self.tasker = splitter.tasker
        self.gcn = gcn
        self.classifier = classifier
        self.comp_loss = comp_loss

        self.num_nodes = dataset.num_nodes
        self.data = dataset
        self.num_classes = num_classes

        self.currdate = str(datetime.datetime.today().strftime('%Y%m%d%H%M%S'))
        self.logger = logger.Logger(args_dic, self.num_classes, logging=logging)

        self.init_optimizers(args_dic)

        self.spark = spark
        self.drop_output_table = drop_output_table

        if self.tasker.is_static:
            adj_matrix = mu.sparse_prepare_tensor(self.tasker.adj_matrix,
                                                  torch_size=[self.num_nodes],
                                                  ignore_batch_dim=False)
            self.hist_adj_list = [adj_matrix]
            self.hist_ndFeats_list = [self.tasker.nodes_feats.float()]

    def init_optimizers(self, args_dic):
        params = self.gcn.parameters()
        self.gcn_opt = torch.optim.Adam(params, lr=args_dic['learning_rate'])
        params = self.classifier.parameters()
        self.classifier_opt = torch.optim.Adam(params, lr=args_dic['learning_rate'])
        self.gcn_opt.zero_grad()
        self.classifier_opt.zero_grad()

    def save_checkpoint(self, state, filename='checkpoint.pth.tar'):
        torch.save(state, filename)

    def load_checkpoint(self, filename, model):
        if os.path.isfile(filename):
            self.logger.logging.info("=> loading checkpoint '{}'".format(filename))
            checkpoint = torch.load(filename)
            epoch = checkpoint['epoch']
            self.gcn.load_state_dict(checkpoint['gcn_dict'])
            self.classifier.load_state_dict(checkpoint['classifier_dict'])
            self.gcn_opt.load_state_dict(checkpoint['gcn_optimizer'])
            self.classifier_opt.load_state_dict(checkpoint['classifier_optimizer'])
            return epoch
        else:
            return 0

    def train(self):
        self.tr_step = 0
        best_eval_valid = 0
        eval_valid = 0
        epochs_without_impr = 0

        for e in range(self.args_dic['num_epochs']):
            eval_train, nodes_embs = self.run_epoch(self.splitter.train, e, 'TRAIN', grad=True)
            if len(self.splitter.dev) > 0 and e > self.args_dic['eval_after_epochs']:
                eval_valid, _ = self.run_epoch(self.splitter.dev, e, 'VALID', grad=False)
                if eval_valid > best_eval_valid:
                    best_eval_valid = eval_valid
                    epochs_without_impr = 0
                    self.logger.logging.info('### w' + str(self.args_dic['rank']) + ') ep ' +
                                             str(e) + ' - Best valid measure:' + str(eval_valid))
                else:
                    epochs_without_impr += 1
                    if epochs_without_impr > self.args_dic['early_stop_patience']:
                        self.logger.logging.info('### w' + str(self.args_dic['rank']) + ') ep ' +
                                                 str(e) + ' - Early stop.')
                        break

            if len(self.splitter.test) > 0 and eval_valid == best_eval_valid and e > self.args_dic['eval_after_epochs']:
                eval_test, _ = self.run_epoch(self.splitter.test, e, 'TEST', grad=False)

                if self.args_dic['save_node_embeddings']:
                    base_tablename = f'{C.KS_CRYPTO_PROJ}:{C.KS_CRYPTO_DS}'
                    self.save_node_embs_csv(nodes_embs, self.splitter.train_idx,
                                            f'{base_tablename}_train_nodeembs_{self.currdate}')
                    self.save_node_embs_csv(nodes_embs, self.splitter.dev_idx,
                                            f'{base_tablename}_valid_nodeembs_{self.currdate}')
                    self.save_node_embs_csv(nodes_embs, self.splitter.test_idx,
                                            f'{base_tablename}_test_nodeembs_{self.currdate}')

    def run_epoch(self, split, epoch, set_name, grad):
        t0 = time.time()
        log_interval = 999
        if set_name == 'TEST':
            log_interval = 1
        self.logger.log_epoch_start(epoch, len(split), set_name, minibatch_log_interval=log_interval)

        torch.set_grad_enabled(grad)
        for s in split:

            s = self.prepare_sample(s)

            predictions, nodes_embs = self.predict(s['hist_adj_list'],
                                                   s['hist_ndFeats_list'],
                                                   s['label_sp']['idx'],
                                                   s['node_mask_list'])

            loss = self.comp_loss(predictions, s.label_sp['vals'])

            self.logger.log_minibatch(predictions, s.label_sp['vals'], loss.detach())

            if grad:
                self.optim_step(loss)

        torch.set_grad_enabled(True)
        eval_measure = self.logger.log_epoch_done()

        return eval_measure, nodes_embs

    def predict(self, hist_adj_list, hist_ndFeats_list, node_indices, mask_list):
        nodes_embs = self.gcn(hist_adj_list,
                              hist_ndFeats_list,
                              mask_list)

        predict_batch_size = 100000
        gather_predictions = []
        for i in range(1 + (node_indices.size(1) // predict_batch_size)):
            cls_input = self.gather_node_embs(nodes_embs,
                                              node_indices[:, i * predict_batch_size:(i + 1) * predict_batch_size])
            predictions = self.classifier(cls_input)
            gather_predictions.append(predictions)
        gather_predictions = torch.cat(gather_predictions, dim=0)
        return gather_predictions, nodes_embs

    def gather_node_embs(self, nodes_embs, node_indices):
        cls_input = []

        for node_set in node_indices:
            cls_input.append(nodes_embs[node_set])
        return torch.cat(cls_input, dim=1)

    def optim_step(self, loss):
        self.tr_step += 1
        loss.backward()

        if self.tr_step % self.args_dic['steps_accum_gradients'] == 0:
            self.gcn_opt.step()
            self.classifier_opt.step()

            self.gcn_opt.zero_grad()
            self.classifier_opt.zero_grad()

    def prepare_sample(self, sample):
        for i, adj in enumerate(sample.hist_adj_list):
            adj = mu.sparse_prepare_tensor(adj, torch_size=[self.num_nodes])
            sample.hist_adj_list[i] = adj.to(self.args_dic['device'])

            nodes = self.tasker.prepare_node_feats(sample.hist_ndFeats_list[i])

            sample.hist_ndFeats_list[i] = nodes.to(self.args_dic['device'])
            node_mask = sample.node_mask_list[i]
            sample.node_mask_list[i] = node_mask.to(self.args_dic['device']).t()

        label_sp = self.ignore_batch_dim(sample.label_sp)

        label_sp['idx'] = label_sp['idx'].to(self.args_dic['device'])

        label_sp['vals'] = label_sp['vals'].type(torch.long).to(self.args_dic['device'])
        sample.label_sp = label_sp

        return sample

    def ignore_batch_dim(self, adj):
        adj['vals'] = adj['vals'][0]
        return adj

    def save_node_embs_csv(self, nodes_embs, indexes, embs_tablename):
        csv_node_embs = []
        for node_id in indexes:
            orig_ID = torch.DoubleTensor([self.tasker.data.contID_to_origID[node_id]])

            csv_node_embs.append(torch.cat((orig_ID, nodes_embs[node_id].double())).detach().numpy())

        embs_df = self.spark.createDataFrame(pd.DataFrame(np.array(csv_node_embs)))\

        if self.drop_output_table:
             # Para crear una tabla nueva
             su.export_to_big_query(embs_df, embs_tablename, mode='overwrite')
        else:
             # Para añadir a una tabla ya existente
             su.export_to_big_query(embs_df, embs_tablename, mode='append')
