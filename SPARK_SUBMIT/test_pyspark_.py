#!/home/ektov1-av_ca-sbrf-ru/bin/python35

import os
import sys
import warnings
warnings.filterwarnings('ignore')

curruser = "ektov1-av_ca-sbrf-ru" #os.environ.get('USER')

if curruser in os.listdir("/opt/workspace/"):
    sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
    sys.path.insert(0, '/opt/workspace/{user}/libs/python3.5/site-packages/'.format(user=curruser))
    # sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata_v1.2/lib/'.format(user=curruser))
else:
    sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
    sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    # sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))

# import re
# import joblib
# import pandas as pd
# import numpy as np
from pathlib import Path
from datetime import datetime as dt
import logging, json

# from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf, HiveContext

import subprocess
dir = os.path.dirname(os.path.realpath(__file__))
print(dir)
print(os.path.realpath(__file__))

class NewSpark:
    def __init__(self):
        self.currdate = dt.strftime(dt.today(), format='%Y-%m-%d')
        # self.sing = tendo.singleton.SingleInstance()
        self.script_name = 'SparkAppJob'
        self.init_logger()
        self.spark_init("SPARK_SUBMIT")

        self.log("# __init__ : begin", self.logger)

    def log(self, message, logger,
            print_log : bool = True):
        if not print_log:
            return

        logger.info(message)
        print(message, file=sys.stdout)
        # print(message, file=sys.stderr)

    def init_logger(self):
        self.print_log = True

        try:
            os.makedirs("/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/SPARK_SUBMIT/logs/" + self.currdate)
        except Exception as ex:
            print("# MAKEDIRS ERROR: \n"+ str(ex), file=sys.stderr)

        log_file='/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/SPARK_SUBMIT/logs/{}/{}.log'.format(self.currdate, self.script_name)
        logger_name = self.script_name
        level=logging.INFO

        self.logger = logging.getLogger(logger_name)
        formatter = logging.Formatter('%(asctime)s : %(message)s')
        fileHandler = logging.FileHandler(log_file, mode='a')
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        self.logger.setLevel(level)
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)

        self.log("="*54 + " {} ".format(self.currdate) + "="*54, self.logger)

    def set_path(self):

        spark_home = '/opt/cloudera/parcels/SPARK2-2.4.0.cloudera2-1.cdh5.13.3.p0.1041012/lib/spark2/'
        os.environ['SPARK_HOME'] = spark_home
        os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO0059623792/bin/python'
        os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO0059623792/bin/python'
        sys.path.insert(0, os.path.join (spark_home,'python'))
        sys.path.insert(0, os.path.join (spark_home,'python/lib/py4j-0.10.7-src.zip'))


    def spark_init(self, spark_app_name, conf=None, replication = '2' ):

        #set_path()
        dynamicAllocationEnabled = False

        conf = SparkConf().setAppName(spark_app_name)

        if not dynamicAllocationEnabled:

            conf = conf.setMaster("yarn").\
            set('spark.local.dir', 'sparktmp').\
            set('spark.executor.memory','20g').\
            set('spark.executor.cores', '8').\
            set('spark.executor.instances', '8').\
            set('spark.sql.parquet.mergeScheme', 'false').\
            set('spark.parquet.enable.summary-metadata', 'false').\
            set('spark.yarn.executor.memoryOverhead', '16g').\
            set('spark.driver.memory','20g').\
            set('spark.driver.maxResultSize','40g').\
            set('spark.yarn.driver.memoryOverhead', '4048mb').\
            set('spark.port.maxRetries', '150').\
            set('spark.dynamicAllocation.enabled', 'false').\
            set('spark.kryoserializer.buffer.max','1g').\
            set('spark.core.connection.ack.wait.timeout', '800s').\
            set('spark.storage.blockManagerSlaveTimeoutMs', '800s').\
            set('spark.shuffle.io.connectionTimeout', '800s').\
            set('spark.rpc.askTimeout', '800s').\
            set('spark.network.timeout', '800s').\
            set('spark.rpc.lookupTimeout', '800s').\
            set('spark.sql.autoBroadcastJoinThreshold', -1).\
            set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true').\
            set('spark.hadoop.dfs.replication', replication).\
            set('spark.sql.execution.arrow.enabled', 'true').\
            set('spark.sql.execution.arrow.pyspark.enabled', 'true')

        elif dynamicAllocationEnabled:

            print("Staring spark context with dynamic allocation...")
            conf = conf.setMaster("yarn").\
            set('spark.port.maxRetries', '150').\
            set('spark.dynamicAllocation.enabled', 'true')

        self.sc = SparkContext.getOrCreate(conf)
        self.hc = HiveContext(self.sc)


if __name__ ==  '__main__':

    sp = NewSpark()
    hc=sp.hc

    CONN_SCHEMA = 'sbx_team_digitcamp'
    CID_INN_TABLE_NAME = 'GA_CID_SBBOL_INN'

    tmp = hc.sql("select CU_ACCOUNT_PROFILE, USER_GUID from {}.GA_MA_USER_PROFILE_SBBOL".format(CONN_SCHEMA))
    res= tmp.groupBy('CU_ACCOUNT_PROFILE').agg(f.countDistinct("USER_GUID").alias("CNT_SBBOLUSERID"))\
       .orderBy("CNT_SBBOLUSERID", ascending=False)

    _dct={}
    lst = [row.asDict() for row in res.collect()]
    for k,v in [(key,d[key]) for d in lst for key in d]:
        if k not in _dct:
            _dct[k]=[v]
        else:
            _dct[k].append(v)

    _str = json.dumps(_dct, ensure_ascii=False, indent=3)

    sp.log("*"*120, sp.logger)
    sp.log(_str, sp.logger)
    sp.log("*"*120, sp.logger)


