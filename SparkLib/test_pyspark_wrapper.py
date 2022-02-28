#!/home/ektov1-av_ca-sbrf-ru/bin/python35
import os
import sys
curruser = os.environ.get('USER')

_labdata = os.environ.get("LABDATA_PYSPARK")
sys.path.insert(0, _labdata)
os.chdir(_labdata)

if curruser in os.listdir("/opt/workspace/"):
    sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/libs/python3.5/site-packages/'.format(user=curruser))
    # sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata_v1.2/lib/'.format(user=curruser))
else:
    sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
    sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    # sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))

#import tendo.singleton
import warnings
warnings.filterwarnings('ignore')

import joblib
import json
from joblib import Parallel, delayed

from time import sleep
from itertools import islice
from multiprocessing import Pool, Process, JoinableQueue
from multiprocessing.pool import ThreadPool
from functools import partial
import subprocess
from threading import Thread
import time
from datetime import datetime

from transliterate import translit

from lib.spark_connector import SparkConnector
from lib.sparkdb_loader import *
from lib.connector import OracleDB
import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame

import re
import pandas as pd
import numpy as np
from tqdm._tqdm_notebook import tqdm_notebook
from pathlib import Path
import shutil
import loader as load

from lib.config import *
from lib.tools import *

# sing = tendo.singleton.SingleInstance()

# os.chdir('/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/Clickstream_Analytics/AutoUpdate/')
# os.chdir('/opt/workspace/{}/notebooks/clickstream/AutoUpdate/'.format(curruser))

def show(self, n=10):
    return self.limit(n).toPandas()

def typed_udf(return_type):
    '''Make a UDF decorator with the given return type'''

    def _typed_udf_wrapper(func):
        return f.udf(func,return_type)

    return _typed_udf_wrapper

pyspark.sql.dataframe.DataFrame.show = show

def print_and_log(message: str):
    print(message)
    logger.info(message)
    return None

CONN_SCHEMA = 'sbx_team_digitcamp' #'sbx_t_team_cvm'


class NewSpark:
    def __init__(self):
        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.sing = tendo.singleton.SingleInstance()
        self.script_name = 'New_Spark_Instance'
        self.init_logger()
        self.start_spark()

        log("# __init__ : begin", self.logger)

    @class_method_logger
    def start_spark(self):
        self.sp = spark(schema=CONN_SCHEMA,
                        process_label='TEST_PYSPARK_',
                        dynamic_alloc=False,
                        replication_num=2,
                        kerberos_auth=True,
                        numofcores=5,
                        numofinstances=10)

        self.hive = self.sp.sql


    def init_logger(self):
        self.print_log = True

        try:
            os.makedirs("./logs/" + self.currdate)
        except:
            pass

        log_file='./logs/{}/{}.log'.format(self.currdate, self.script_name)
        logger_name = self.script_name
        level=logging.INFO

        self.logger = logging.getLogger(logger_name)
        formatter = logging.Formatter('%(asctime)s : %(message)s')
        fileHandler = logging.FileHandler(log_file, mode='w')
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        self.logger.setLevel(level)
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)

        log("="*54 + " {} ".format(self.currdate) + "="*54, self.logger)


    def __enter__(self):
        return self

    def close(self):
        try:
            self.sp.sc.stop()
        except Exception as ex:
            if "object has no attribute" in str(ex):
                log("### SparkContext was not started", self.logger)
            else:
                log("### Close exception: \n{}".format(ex), self.logger)
        finally:
            del self.sing


    def __exit__(self, exc_type, exc_value, exc_tb):
        log("# __exit__ : begin", self.logger)
        if exc_type is not None:
            log("### Exit exception ERROR:\nexc_type:\n{}\nexc_value:\n{}\nexc_tb:\n{}"\
                .format(exc_type, exc_value, exc_tb), self.logger)
        self.close()
        log("# __exit__ : end", self.logger)
        log("="*120, self.logger)



if __name__ == '__main__':

    print("### Instantiating Child of spark class oblect. Run")
    with NewSpark() as newcl:
        print(newcl.sp)

    print("### Starting static spark context. Kerberos AUTH = True!")

    with spark(schema=CONN_SCHEMA,
               dynamic_alloc=False,
               numofinstances=5,
               numofcores=8,
               executor_memory='25g',
               driver_memory='25g',
               kerberos_auth=True,
               process_label="TEST_PYSPARK_"
               ) as sp:

        hive = sp.sql
        print(sp.sc.version)

    print("### Starting static spark context. Kerberos AUTH = False!")

    with spark(schema=CONN_SCHEMA,
               dynamic_alloc=False,
               numofinstances=5,
               numofcores=8,
               executor_memory='25g',
               driver_memory='25g',
               kerberos_auth=False,
               process_label="TEST_PYSPARK_"
               ) as sp:

        hive = sp.sql
        print(sp.sc.version)


    print("### Starting static spark context from user-defined template. Run!")

    with spark(schema=CONN_SCHEMA,
               usetemplate_conf = True,
               default_template= PYSPARK_HIVE_STATIC_KRBR_TGT,
               process_label="TEST_PYSPARK_"
               ) as sp:

        hive = sp.sql
        print(sp.sc.version)

