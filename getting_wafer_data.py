#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 13:32:47 2023

@author: ruchak
"""
import numpy as np
from io import BytesIO
from utils import *
import requests
import Config as cfg
from PIL import Image
#import numpy as np
from datetime import date, timedelta, datetime
import os
import pandas as pd
import shutil
import subprocess
import sys
from threading import Thread
import smtplib
import re


def getInputData():
    # get the data
    schema = 'enseapedia'
    table = 'jmp_cdsem'
    edw_query = f"""select 
                        *
                    from 
                        {schema}.{table} 
                    where
                    wafercode in ('6P', '2W') and (lp_slope_target<1)
                """
    reli_df = None
    nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
    reli_df = nrm_ifc.read(edw_query)
    return reli_df


reli_df = getInputData()


l1 = sorted(os.listdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/marlin results/1.abnormal/'))

l1 = [x[:-4] for x in l1]


# get the data
schema = 'hamr_ida_rw'
table = 'cdsem_anomaly_detection_results'
edw_query = f"""select 
                    HD_NUM
                from 
                    {schema}.{table} 
            """
my_df = None
nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
my_df = nrm_ifc.read(edw_query)['HD_NUM'].to_list()
l1 = l1 + my_df
hd_nums = list(set(reli_df['HD_NUM'].to_list()).difference(set(l1)))

reli_df = reli_df[reli_df['HD_NUM'].isin(hd_nums)]

reli_df.to_csv('/home/ruchak/reli_df.csv', index = False)
                                                            