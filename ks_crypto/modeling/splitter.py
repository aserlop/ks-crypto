from torch.utils.data import Dataset, DataLoader
import torch
import numpy as np


class splitter():

    def __init__(self, args_dic, tasker):

        self.train_proportion = args_dic['train_proportion']
        self.dev_proportion = args_dic['dev_proportion']

        assert self.train_proportion + self.dev_proportion < 1, \
            'there\'s no space for test samples'

        # only the training one requires special handling on start, the others are fine with the split IDX.
        start = tasker.data.min_time + tasker.num_hist_steps  # -1 + args.adj_mat_time_window
        end = self.train_proportion

        end = int(np.floor(tasker.data.max_time.type(torch.float) * end))
        train = data_split(tasker, start, end, test=False)
        train = DataLoader(train, batch_size=1, num_workers=2)

        start = end
        end = self.dev_proportion + self.train_proportion
        end = int(np.floor(tasker.data.max_time.type(torch.float) * end))

        dev = data_split(tasker, start, end, test=True)

        dev = DataLoader(dev, num_workers=2)

        start = end

        # the +1 is because I assume that max_time exists in the dataset
        end = int(tasker.max_time) + 1

        test = data_split(tasker, start, end, test=True)

        test = DataLoader(test, num_workers=2)

        self.tasker = tasker
        self.train = train
        self.dev = dev
        self.test = test


class data_split(Dataset):
    def __init__(self, tasker, start, end, test, **kwargs):
        self.tasker = tasker
        self.start = start
        self.end = end
        self.test = test
        self.kwargs = kwargs

    def __len__(self):
        return self.end - self.start

    def __getitem__(self, idx):
        idx = self.start + idx
        t = self.tasker.get_sample(idx, test=self.test, **self.kwargs)
        return t
