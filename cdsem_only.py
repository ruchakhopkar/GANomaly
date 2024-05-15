#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr  8 23:45:00 2023

@author: ruchak
"""
import os
import requests
import numpy as np
from PIL import Image
from io import BytesIO
import pandas as pd
from datetime import date
import re
import shutil
from dbs import *

def cleanData(source_df):
    '''
    cleanData function is going to remove all rows with no image url and give wafercodes and lot ids

    Parameters
    ----------
    source_df : updated source df
        DataFrame that is an output from keepLatestData

    Returns
    -------
    images : list of image urls to process
        
    hd_num : head serial numbers of all the images
        
    dates : insert date of peg images retrieved from the source table
        
    wafercode : wafercodes of all heads
        
    wafer_substrate_lot_id : wafer substrate lot IDs of all heads
    
    wfr_hamr_design : wafer HAMR designs for all heads
    
    slider_lot_num: list of all slider lot Numbers for all heads

    '''
    print(source_df)
    source_df = source_df[source_df['URL_POLE_METROLOGY'].notna()]
    images = source_df['URL_POLE_METROLOGY'].tolist()
    hd_num = source_df['HD_NUM'].tolist()
    dates = source_df['INSERTCDSEMDATE'].tolist()
    wafercode = source_df['WAFERCODE'].tolist()
    wafer_substrate_lot_id = source_df['WAFER_SUBSTRATE_LOT_ID'].tolist()
    wfr_hamr_design = source_df['WFR_HAMR_DESIGN'].tolist()
    slider_lot_num = source_df['SLIDER_LOT_NUM'].tolist()
    return images, hd_num, dates, wafercode, wafer_substrate_lot_id, wfr_hamr_design, slider_lot_num
    
def storeData(images, hd_num):
    '''
    Downloads and stores peg images in the anomaly folder

    Parameters
    ----------
    images : List of URLs to download from.
        
    hd_num : List of head numbers.

    Returns
    -------
    None.

    '''
    # remove existing test files and output directory
    try:
        os.system('rm -rf -v /home/ruchak/ganomaly-master/data/cdsem/test/1.abnormal')
    except:
        pass
    try:
        os.system('rm -rf -v /home/ruchak/ganomaly-master/output/ganomaly/cdsem/test/images')
    except:
        pass
    
    if not(os.path.exists(os.path.join('/home/ruchak/ganomaly-master/data/cdsem/test/', '1.abnormal'))):
        os.mkdir('/home/ruchak/ganomaly-master/data/cdsem/test/1.abnormal')
    
    
    #download new images
    for img in range(len(images)):
        try:
            im = Image.open(BytesIO(requests.get(images[img], verify = False).content))
            im.save('/home/ruchak/ganomaly-master/data/cdsem/test/1.abnormal/' + hd_num[img] + '.png')
            
        except Exception as e:
            print(e)
            pass
    
        
    os.environ['MKL_THREADING_LAYER'] = 'GNU'
    os.system('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/ruchak/anaconda3/lib/')

def runModels():
    '''
    Runs the ROD and anomaly detection system on peg CDSEM images.

    Returns
    -------
    None.

    '''

    
    #running the anomaly detection system
    os.system('python /home/ruchak/ganomaly-master/train.py --batchsize 1 --dataset cdsem --isize 64 --phase test --save_test_images --niter 1 --nz 125 --lr 0.0002 --w_con 80 --load_weights --dataroot /home/ruchak/ganomaly-master --outf /home/ruchak/ganomaly-master/output')
    

def storeResults(hd_num, dates, images, wafercode, wafer_substrate_lot_id, wfr_hamr_design, slider_lot_num):
    '''
    

    Parameters
    ----------
    dates : List of insert_dates
        
    images : List of image links to download images from.
        
    wafercode : List of Wafercode derived from head numbers
        
    wafer_substrate_lot_id : List of Wafer substrate lot IDs derived from head numbers
        DESCRIPTION.
        
    wfr_hamr_design: List of wafer HAMR designs for all heads 
    
    slider_lot_num : List of slider lot numbers for all heads

    Returns
    -------
    None.

    '''
    #getting peg ROD results
    cdsem_res = pd.DataFrame()
    cdsem_res['HD_NUM'] = hd_num
    cdsem_res['INSERTCDSEMDATE'] = dates
    cdsem_res['URL_POLE_METROLOGY'] = images
    cdsem_res['WAFERCODE'] = wafercode
    cdsem_res['WAFER_SUBSTRATE_LOT_ID'] = wafer_substrate_lot_id
    cdsem_res['LAST_UPDATE_DATE'] = date.today()
    cdsem_res['SLIDER_LOT_NUM'] = slider_lot_num
    cdsem_res['WFR_HAMR_DESIGN'] = wfr_hamr_design
    #getting anomaly detection results
    actual_hds = sorted(os.listdir('/home/ruchak/ganomaly-master/data/cdsem/test/1.abnormal/'))
    actual_hds = [i[:-4] for i in actual_hds]
    cdsem_res = cdsem_res[cdsem_res['HD_NUM'].isin(actual_hds)]
    cdsem_res = cdsem_res.sort_values(by = 'HD_NUM', ignore_index = True)
    
    #get anomaly scores
    anomaly_scores = np.load('/home/ruchak/ganomaly-master/output/ganomaly/cdsem/test/an_scores.npy', allow_pickle=False)[254:]
    cdsem_res['PREDICTIONS'] = anomaly_scores
    cdsem_res['DEFINITELY_ANOMALOUS'] = np.where(anomaly_scores>0.42, 1, 0)
    cdsem_res['HARD_THRESHOLDING_RESULTS'] = np.where(anomaly_scores>0.23, 1, 0)
    print(anomaly_scores, cdsem_res)
    
    
    output_links = []
    for i in range(len(cdsem_res)):
            print(re.findall(r'[^\/\\]+(?=\.png|\.jpg)', str(cdsem_res['URL_POLE_METROLOGY'].iloc[i])))
            image_name = re.findall(r'[^\/\\]+(?=\.png|\.jpg)', str(cdsem_res['URL_POLE_METROLOGY'].iloc[i]))[0]
            src = '/home/ruchak/ganomaly-master/output/ganomaly/cdsem/test/images/'+image_name.split('-')[0]+'.png'
    
            dest = cdsem_res['URL_POLE_METROLOGY'].iloc[i]
            server = dest[:dest.find('.nrm.minn.seagate.com')].split('//')[1]
            dest = dest.replace('http://'+server+'.nrm.minn.seagate.com/~gtx/', '/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/')
            if os.path.exists(src):
                os.makedirs(os.path.dirname(dest), mode = 0o777, exist_ok=True)
                shutil.copy(src, dest)
                output_links.append(cdsem_res['URL_POLE_METROLOGY'].iloc[i].replace('http://'+server + '.nrm.minn.seagate.com/~gtx/', 'http://ida.seagate.com:9081/image_repo/slider_CDSEM_anomaly/'))
    cdsem_res['OUTPUT_LINKS'] = output_links
    cdsem_res['LAST_UPDATE_DATE'] = pd.to_datetime(cdsem_res['LAST_UPDATE_DATE'])
    cdsem_res['INSERTCDSEMDATE'] = pd.to_datetime(cdsem_res['INSERTCDSEMDATE'])
    for i in range(0, len(cdsem_res), 1000):
        try:
            subset = cdsem_res[i:i+1000]
        except:
            subset = cdsem_res[i:]
        upsert_to_edw_prep(subset, 'hamr_ida_rw', 'cdsem_anomaly_detection_results')
    

     
    
