"""
LOAD DATA from file.
"""

# pylint: disable=C0301,E1101,W0622,C0103,R0902,R0915

##
import os
import torch
import numpy as np
import torchvision.datasets as datasets
from torchvision.datasets import ImageFolder
import torchvision.transforms as transforms
from torch.utils.data import Dataset
##
    
def load_data(opt):
    """ Load Data

    Args:
        opt ([type]): Argument Parser

    Raises:
        IOError: Cannot Load Dataset

    Returns:
        [type]: dataloader
    """

    ##
    # LOAD DATA SET
    if opt.dataroot == '':
        opt.dataroot = './data/{}'.format(opt.dataset)
    else:
        opt.dataroot = opt.dataroot+'/data/{}'.format(opt.dataset)
        
    splits = ['train', 'test']
    drop_last_batch = {'train': True, 'test': False}
    shuffle = {'train': True, 'test': False}
    transform = transforms.Compose([transforms.Resize(opt.isize),
                                    transforms.CenterCrop(opt.isize),
                                    transforms.ToTensor(),
                                    transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5)), ])
    
    dataset = {x: ImageFolder(os.path.join(opt.dataroot, x), transform) for x in splits}
    opt.datasets_names = {x:dataset[x].imgs for x in splits}
    dataloader = {x: torch.utils.data.DataLoader(dataset=dataset[x],
                                                 batch_size=opt.batchsize,
                                                 shuffle=shuffle[x],
                                                 num_workers=int(opt.workers),
                                                 drop_last=drop_last_batch[x],
                                                 worker_init_fn=(None if opt.manualseed == -1
                                                 else lambda x: np.random.seed(opt.manualseed)))
                  for x in splits}
    return dataloader


