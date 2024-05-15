from dbs import *
from cdsem_only import *

def job():
        
    try:
        source_df = getInputData('enseapedia', 'jmp_cdsem', 'hamr_ida_rw', 'cdsem_anomaly_detection_results', ['hd_num', 'url_pole_metrology', 'insertcdsemdate', 'wafercode', 'wfr_hamr_design', 'slider_lot_num', 'wafer_substrate_lot_id'], head_column = 'HD_NUM', \
                                 link_col = 'URL_POLE_METROLOGY')
        
        
        
        images, hd_num, dates, wafercode, wafer_substrate_lot_id, wfr_hamr_design, slider_lot_num = cleanData(source_df)
        
        
        storeData(images, hd_num)
        
        runModels()
        
        storeResults(hd_num, dates, images, wafercode, wafer_substrate_lot_id, wfr_hamr_design, slider_lot_num)
        print('Run Completed')
    except:
        pass

import time
import schedule

schedule.every().day.at('10:14').do(job)

while 1:
    schedule.run_pending()
    time.sleep(1)
  

                                                                                                                                                                                                       
