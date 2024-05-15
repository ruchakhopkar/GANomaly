#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# Author        : Sathish Raman(539188)
# Copyright     : Copyright (c) 2020 Seagate Technology | Charecterisation Analytics Group (CAG)
# Script Name   : FM101 Configurations
# Version       : M1 checkout | Mar 2022
# Email         : sathish.raman@seagate.com
# Status        : Production
# Maintainer    : Sathish Raman
# License       : Seagate Internal Use Only
# Description   : A config module that contains constants and main program settings
"""

'''
Global Constants
'''
APP_NAME    = 'FM101 IDA Version'
VERSION     = 'M1 checkout | Mar 2022'

'''Logging Configurations'''
env_key='LOG_CFG'
path = 'logging.json'

'''Oracle Database Configurations'''
orc_db_name     = 'hamr_ida_rw'
orc_username    = 'HAMR_IDA_RW'        #'539188'     
orc_passwd      = 'CAGida2020'         #'Seagate1234'
orc_hostname    = 'okpedw1.okla.seagate.com'
orc_port        = 1531
orc_sid         = 'okpedw1'
orc_encoding    = 'UTF-8'

'''Oracle TTODS Database Configurations'''
orc_ods_db_name     = 'IDA_DEV'
orc_ods_username    = 'IDA_DEV'
orc_ods_passwd      = 'seagate'
orc_ods_hostname    = 'ttods.tep.thai.seagate.com'
orc_ods_port        = 1521
orc_ods_sid         = 'ods'
orc_ods_encoding    = 'UTF-8'


'''MySQL Database Configurations'''
mys_db_name     = 'test_db'
mys_username    = 'ida'
mys_passwd      = 'Seagate2019<,'
mys_hostname    = 'ida-00.nrm.minn.seagate.com'
mys_port        = 3306

'''MySQL Database Configurations'''
mys_tep_db_name     = 'test_db_new'
mys_tep_username    = 'ida'
mys_tep_passwd      = 'Seagate2019<,'
mys_tep_hostname    = '10.44.95.80'
mys_tep_port        = 3306

'''BER IOP25 Spec'''
spec_dict = {
    'AP2.0.3':0,
    'AP2.1.4':0,
    'AP2.5.1':8.5,
    'AP3.2':13,
    'AP3.3':13,
    'AP3.4':13,
    'AP3.5':13
}

# add TT VP P9 SBR list here
priority_alive_sbrs = [
    # 'TT6TP210170','TT6TP210171','TT6TP210172','TT6TP210174','TT6TP210175','TT6TP210176',
    # 'TT6TP210230','TT6TP210231','TT6TP210232','TT6TP210234','TT6TP210235','TT6TP210236','TT6TP210237','TT6TP210238','TT6TP210239',
    # 'TT6TP210057','TT6TP210058','TT6TP210061','TT6TP210059','TT6TP210060','TT6TP210062','TT6TP210287','TT6TP210289' #P9 BINS
]

relia_log_table_ids = {
     'P_DVL_LIFE_TEST_CYCLE_3'  :'03',
     'P_DVL_810_LT_RD_REF_TUB'  :'05',
     'P_DVL_812_LT_SQZ_REF_TUB' :'07',
     'P_DVL_815_BER_VS_IOP_DAC' :'10',
     'P_LIFE_TEST_SUMMARY_D3'   :'13',
     'P228_IBIAS_OSC_DETCR_DATA':'14',
     'P228_IBIAS_OSC_SUMMARY'   :'28'

}


repo_flag = 'RELI'

repo_source_path = {
    # 'RELI'   : '/extrastg/mdfs14/hamr-analysis/log/ALL_DRIVE/sn/',
    # 'RELI'  : '/app/SP_Test/',
    'RELI'  : '/app/FM101/fmc_process/output/',
    'OC'     : '/mnt/tep/WDVL/TEST_SERVER/CLICKER_PROJECT/DATA_LIVE/',
    'CUSTOM' : '/home/sathish/Automation_Scripts/',
    'DEFAULT': './' 
}

