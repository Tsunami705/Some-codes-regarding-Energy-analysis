#!/usr/bin/env python3

import os, logging

from datetime import datetime

class ApiConf:
    WORK_DIR = '/home/node/proj/hbaseHttp'              # work dir
    SP_HOME = '/opt/apps/spark-3.3.0'                   # Spark dir
    
    IN_FILE = 'input.json'                              # input file name
    OUT_FILE = 'output.json'                            # output file name
    
    HDFS_URL = 'http://unode01:9870'                    # HDFS link path
    ZK_URL = 'unode04:2181'                             # Zookeeper link path
    PH_URL = 'http://unode04:8765'                      # Phoenix link path
    SP_URL = 'spark://unode01:7077'                     # Spark link path
    
    def __init__(self):
        ''' initialized Api link path '''
        pass
    
    def get_file_logger(self, name):
         ''' Get the logger
        
             parameter:
                 name: logger name
                
             return:
                 Recorder
         '''
         os.chdir(f'{ApiConf.WORK_DIR}/log')
         filename = os.path.abspath( # log name
             f"hbHttp-{datetime.now().strftime('%y%m%d')}.log"
         )
         logger = logging.getLogger(name) # logger
         logger. setLevel(logging. ERROR)
        
         formatter = logging.Formatter( # formatter
             '[%(asctime)s] [%(filename)s:%(lineno)d]'
             '[%(levelname)s] - %(message)s'
         )
        
         handler = logging.FileHandler(filename, encoding='UTF-8') # processor
         handler. setLevel(logging. ERROR)
        
         handler. setFormatter(formatter)
         logger. addHandler(handler)
        
         return logger
