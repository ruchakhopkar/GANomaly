#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 26 09:55:32 2023

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
def upsert_to_edw_prep(df,schema,table):
    
    hd_num_text = ", 0), (".join(map(str,df['HD_NUM'].unique()))
    
    edw_query = f"""select 
                        hd_num
                    from 
                        hamr_ida_rw.cdsem_anomaly_detection_results 
                    where 
                        (hd_num,0) in (({hd_num_text}, 0))
                 """
    reli_df = pd.DataFrame()
    schema = 'hamr_ida_rw'
    table = 'cdsem_anomaly_detection_results'
    try:
        nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
        reli_df = nrm_ifc.read(edw_query)
        if reli_df is None:
            reli_df = pd.DataFrame()
        else:
            reli_df.columns = reli_df.columns.str.upper()
        logger.info(f"Reading {table} data into EDW database successfully completed")
    except Exception as ex:
        logger.error(f"Error occured while reading {table} table data in EDW",exc_info=True)
        insert_error_tracker(f"Error occured while reading {table} table data in EDW")
        
    if reli_df.shape[0]==0:
        df['STATUS'] = 'INSERT'
    else:
        reli_df['STATUS'] = 'UPDATE'
        df = df.merge(reli_df,how='left', on = 'HD_NUM')
        df['STATUS'] = df['STATUS'].fillna('INSERT')

    """2. Split into insert and update dataframes"""
    df_insert = df[df['STATUS']=="INSERT"]
    df_update = df[df['STATUS']=="UPDATE"]

    """3. Insert to EDW"""
    if df_insert.shape[0]>0:
        logger.info("Inserting the new heads")
        df_insert = df_insert.drop(['STATUS'],axis=1)
        df_insert = df_insert.where(pd.notnull(df_insert), None) #convert nan values to oracle null before insert
        cols_list = df_insert.columns
        edw_query = f"""INSERT INTO {schema}.{table} ("""
        for idx,cols in enumerate(cols_list):
            if not idx == len(cols_list)-1:
                edw_query +=cols+','
            else:
                edw_query +=cols
        edw_query +=") VALUES ("
        for idx,cols in enumerate(cols_list):
            if not idx == len(cols_list)-1:
                edw_query +=':'+cols+','
            else:
                edw_query +=':'+cols
        edw_query +=  ')'
        try:
            nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
            params = list(df_insert.itertuples(index=False,name=None))

            nrm_ifc.batch_insert(edw_query,params)
            logger.info(f"Inserting {schema}.{table} data into EDW database successfully completed")
        except Exception as ex:
            logger.error("Error occured while inserting {table} table data in EDW".format(table=table),exc_info=True)
            insert_error_tracker("Error occured while inserting {table} table data in EDW".format(table=table))

    """3. Update to EDW"""
    if df_update.shape[0]>0:
        logger.info("Updating the existing heads")
        df_update = df_update.drop(columns = ['STATUS'], axis = 1)
        df_update = df_update.where(pd.notnull(df_update), None) #convert nan values to oracle null before insert
        cols_list = df_update.columns

        count=0
        for index,row in df_update.iterrows():
            sbr = df_update['HD_NUM'].iloc[count]
            
            edw_query = f"""UPDATE hamr_ida_rw.cdsem_anomaly_detection_results SET """
            params = []
            count+=1
            for idx,cols in enumerate(cols_list):
                # params.append(row[cols])
                if not idx == len(cols_list)-1:
                    # print(type(row[cols]))
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"',"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '"+ str(row[cols])+"'"+","
                        else:
                            edw_query +=cols+'='+str(row[cols])+","
                    # else:
                    #     edw_query +=cols+'='+row[cols]+","
                else:
                    if(row[cols]!=None):
                        if(isinstance(row[cols], str)):
                            edw_query +=cols+"='"+str(row[cols])+"'"
                        elif 'Timestamp' in str(type(row[cols])):
                            edw_query +=cols+"="+"TIMESTAMP '" + str(row[cols])+"'"
                        else:
                            edw_query +=cols+'='+str(row[cols])
                    # else:
                    #     edw_query +=cols+'='+row[cols]
                    # edw_query +=cols+'='+str(row[cols])
            if edw_query.endswith(","):
                edw_query = edw_query[:-1]
            edw_query +=f" WHERE HD_NUM = '{sbr}'"
            try:
                nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
                # params = list(df_update.itertuples(index=False,name=None))
                params=list(tuple(params))
                # print(len(params[0]),len(cols_list))
                # print(edw_query)
                nrm_ifc.insert(edw_query,params)
                logger.info("Updating {table} data into EDW database successfully completed".format(table=table))
            except Exception as ex:
                logger.error("Error occured while Updating {table} table data in EDW".format(table=table),exc_info=True)
                insert_error_tracker("Error occured while Updating {table} table data in EDW".format(table=table))

def getInputData():
    # get the data
    schema = 'enseapedia'
    table = 'jmp_cdsem'
    edw_query = f"""select 
                        *
                    from 
                        {schema}.{table} 
                """
    reli_df = None
    nrm_ifc = DAO_Interface(cfg.orc_username,cfg.orc_passwd,cfg.orc_hostname,cfg.orc_sid,cfg.orc_port,cfg.orc_encoding)
    reli_df = nrm_ifc.read(edw_query)
    return reli_df


def keepLatestData(reli_df):
    #keep only the latest data
    date1 = date.today()
    yesterday = sorted(os.listdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'))
    try:
        yesterday.remove('marlin results')
    except:
        pass
    try:
        yesterday.remove('summary.csv')
    except:
        pass
    try:
        yesterday.remove('slider')
    except:
        pass
    print(yesterday)
    #yesterday = '2023-03-02'
    reli_df = reli_df[reli_df['INSERTCDSEMDATE'] > datetime.strptime(yesterday[-1] +' 00:00:00', '%Y-%m-%d %H:%M:%S')]
    # reli_df = reli_df[reli_df['INSERTCDSEMDATE'] < datetime.strptime('2023-03-27' +' 10:08:00', '%Y-%m-%d %H:%M:%S')]
    #reli_df = reli_df[reli_df['INSERTCDSEMDATE'] < '2023-03-17 ']
    images = reli_df['URL_POLE_METROLOGY'].tolist()
    wafercode = reli_df['WAFERCODE'].tolist()
    wfr_hamr_design = reli_df['WFR_HAMR_DESIGN'].tolist()
    slider_lot_num = reli_df['SLIDER_LOT_NUM'].tolist()
    wafer_substrate_lot_id = reli_df['WAFER_SUBSTRATE_LOT_ID'].tolist()
    dates = reli_df['INSERTCDSEMDATE'].tolist()
    hd_num = reli_df['HD_NUM'].tolist()
    print(len(dates))
    return images, date1, wafercode, wfr_hamr_design, slider_lot_num, wafer_substrate_lot_id, dates, hd_num, reli_df

def storeData(date1, images, hd_num, reli_df):
        try:
           if os.path.exists(os.path.join('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/', str(date1))):
              if len(os.listdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1)+'/output_0.23')) == 0:
                 pass
              else:
                 os.mkdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1))
           else:
              os.mkdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1))
        except:
            print('Run has already completed')
            sender = 'reply-to.ida.hamr@seagate.com'
            receivers = ['rucha.khopkar@seagate.com']
   
    
            message = """From: no-reply-ida-hamr <reply-to.ida.hamr@seagate.com>
            Subject: SMTP e-mail test""" + '\n\n'+  """CDSEM run has already completed for the day."""
    
            try:
                smtpObj = smtplib.SMTP('mailhost.seagate.com')
                smtpObj.sendmail(sender, receivers, message)
                print("Successfully sent email")
            except smtplib.SMTPException:
                print("Error: unable to send email")
    
            sys.exit()
        if not(os.path.exists(os.path.join('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1), '/1.abnormal'))):
            os.mkdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1) + '/1.abnormal')
        if not(os.path.exists(os.path.join('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1), '/output_0.23'))):
            os.mkdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1) + '/outputs_0.23')
        if not(os.path.exists(os.path.join('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1), '/outputs_0.42'))):
            os.mkdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1) + '/outputs_0.42')
        print(images)
        for img in range(len(images)):
            try:
                x = np.array(Image.open(BytesIO(requests.get(images[img], verify = False).content)))
                print('got array')
                im = Image.fromarray(x)
                print('got image')
                im.save('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+ str(date1) + '/1.abnormal/' + hd_num[img] + '.png')
                
            except Exception as e:
                print(e)
                pass
    
    
        #remove existing test files and output directory
        try:
            os.system('rm -rf -v /home/ruchak/ganomaly-master/data/cdsem/test/1.abnormal')
        except:
            pass
        try:
            os.system('rm -rf -v /home/ruchak/ganomaly-master/output/ganomaly/cdsem/test/images')
        except:
            pass
        
        #copy new test images 
        os.system('cp -vR /extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/' + str(date1) + '/1.abnormal/' 
                  ' /home/ruchak/ganomaly-master/data/cdsem/test/1.abnormal/')
        
        #remove current an and gt scores
        try:
            os.system('rm /home/ruchak/ganomaly-master/an_scores.npy')
        except:
            pass
        try:
            os.system('rm /home/ruchak/ganomaly-master/gt_scores.npy')
        except:
            pass
        
        os.environ['MKL_THREADING_LAYER'] = 'GNU'

def storeResults(images, date1, wafercode, wfr_hamr_design, slider_lot_num, wafer_substrate_lot_id, dates):
    #tar the output and store in the apache webserver
    links = pd.DataFrame()
    #print(images)
    hds, output_links, input_links, wfrcode, wfrdesign, sliderlot, wafer_substrate, insertcdsemdates = [], [], [], [], [], [], [], []
    for i in range(len(images)):
        #print(images[i])
        image_name = re.findall(r'[^\/\\]+(?=\.png|\.jpg)', images[i])[0]
        src = '/home/ruchak/ganomaly-master/output/ganomaly/cdsem/test/images/'+image_name.split('-')[0]+'.png'
        dest = images[i].replace('http://gtx9.nrm.minn.seagate.com/~gtx/', '/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/')  
        
        if os.path.exists(src):
            #print('hi')
            #original_umask = os.umask(0)
            #print(os.path.dirname(dest))
            os.makedirs(os.path.dirname(dest), mode = 0o777, exist_ok=True)
            #os.umask(original_mask)
            shutil.copy(src, dest)
            hds.append(image_name.split('-')[0])
            output_links.append(images[i].replace('http://gtx9.nrm.minn.seagate.com/~gtx/', 'http://ida.seagate.com:9081/image_repo/slider_CDSEM_anomaly/'))
            input_links.append(images[i])
            wfrcode.append(wafercode[i])
            wfrdesign.append(wfr_hamr_design[i])
            sliderlot.append(slider_lot_num[i])
            wafer_substrate.append(wafer_substrate_lot_id[i])
            insertcdsemdates.append(dates[i])
    print(hds)
    links['HD_NUM'] = hds
    links['OUTPUT_LINKS'] = output_links
    links['URL_POLE_METROLOGY'] = input_links
    links['WAFERCODE'] = wfrcode
    links['WFR_HAMR_DESIGN'] = wfrdesign
    links['SLIDER_LOT_NUM'] = sliderlot
    links['WAFER_SUBSTRATE_LOT_ID'] = wafer_substrate
    links['INSERTCDSEMDATE'] = insertcdsemdates
        # try:
        #      paths = img.split('//')[1].split('/')[2:-1]
        #      for pth in range(len(paths)+1):
        #          path = paths[:pth]
        #          if not(os.path.exists(os.path.join('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/', '/'.join(path)))):
        #              os.mkdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+ '/'.join(path))
        #      shutil.copy(os.path.join('/home/ruchak/ganomaly-master/output/ganomaly/cdsem/test/images',img.split('//')[1].split('/')[-1].split('-')[0]+'.png'), '/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+ '/'.join(paths)+'/'+img.split('//')[1].split('/')[-1])
        #      output_links.append('http://ida.seagate.com:9081/image_repo/slider_CDSEM_anomaly/'+ '/'.join(paths)+'/'+img.split('//')[1].split('/')[-1])
        #      hds.append(img.split('//')[1].split('/')[-1].split('-')[0])
        # except:
        #      pass

    
    
    #build a csv of the results with the thresholds
    an = np.load('/home/ruchak/ganomaly-master/output/ganomaly/cdsem/test/an_scores.npy', allow_pickle=False)[254:]
    
    df = pd.DataFrame()
    x = sorted(os.listdir('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1) + '/1.abnormal'))
    df['Filename'] = x
    df['PREDICTIONS'] = an
    df['DEFINITELY_ANOMALOUS'] = np.where(an>0.42, 1, 0)
    df['HARD_THRESHOLDING_RESULTS'] = np.where(an>0.23, 1, 0)
    df['HD_NUM'] = df['Filename'].str[:-4]
    print(df.head(5))
    print(links.head(5))
    df = df.merge(links, on = 'HD_NUM')
    df = df.drop(columns = ['Filename'])
    df.to_csv('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1) + '/results.csv', index = False)
    
    definitely_anomalous = df.iloc[np.where(df['DEFINITELY_ANOMALOUS']==1)[0], :]['HD_NUM']
    
    for file in definitely_anomalous:
        shutil.copy('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/' + str(date1) + '/1.abnormal/' +file + '.png',
                    '/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1) + '/outputs_0.42/'+file + '.png')
    
    hard_thresholding = df.iloc[np.where(df['HARD_THRESHOLDING_RESULTS']==1)[0], :]['HD_NUM']
    
    for file in hard_thresholding:
        shutil.copy('/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/' + str(date1) + '/1.abnormal/' +file+'.png',
                    '/extrastg/mdfs14/hamr-analysis/image_repo/slider_CDSEM_anomaly/'+str(date1) + '/outputs_0.23/'+file + '.png')
    return df
def job():

    
    reli_df = getInputData()
    images, date1, wafercode, wfr_hamr_design, slider_lot_num, wafer_substrate_lot_id, dates, hd_num, reli_df = keepLatestData(reli_df)
    

    if len(reli_df)>0:
        
        storeData(date1, images, hd_num, reli_df)
        #get predictions
        os.system('python /home/ruchak/ganomaly-master/train.py --batchsize 1 --dataset cdsem --isize 64 --phase test --save_test_images --niter 1 --nz 125 --lr 0.0002 --w_con 80 --load_weights')
        
        
        store_df = storeResults(images, date1, wafercode, wfr_hamr_design, slider_lot_num, wafer_substrate_lot_id, dates)

        schema = 'hamr_ida_rw'
        table = 'cdsem_anomaly_detection_results'
        upsert_to_edw_prep(store_df, schema, table)
        
        sender = 'reply-to.ida.hamr@seagate.com'
        receivers = ['rucha.khopkar@seagate.com']
    
    
        message = """From: no-reply-ida-hamr <reply-to.ida.hamr@seagate.com>
        Subject: SMTP e-mail test""" + '\n\n'+ """CDSEM anomaly detection system is done running! New results are present."""
        
        
        try:
            smtpObj = smtplib.SMTP('mailhost.seagate.com')
            smtpObj.sendmail(sender, receivers, message)
            print("Successfully sent email")
        except smtplib.SMTPException:
            print("Error: unable to send email")
    
    else:
        sender = 'reply-to.ida.hamr@seagate.com'
        receivers = ['rucha.khopkar@seagate.com']
    
    
        message = """From: no-reply-ida-hamr <reply-to.ida.hamr@seagate.com>
        Subject: SMTP e-mail test""" + '\n\n' + """No new CDSEM data is present"""
    
        try:
            smtpObj = smtplib.SMTP('mailhost.seagate.com')
            smtpObj.sendmail(sender, receivers, message)
            print("Successfully sent email")
        except smtplib.SMTPException:
            print("Error: unable to send email")
    return

#job()
import time
import schedule

schedule.every().day.at('12:50').do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
