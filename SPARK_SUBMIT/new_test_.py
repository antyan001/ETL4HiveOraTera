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

import re
# import joblib
# import pandas as pd
# import numpy as np
from pathlib import Path
from datetime import datetime as dt
import logging, json

# from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
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
        self.local_path = "/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/SPARK_SUBMIT/logs/"
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
            os.makedirs(self.local_path + self.currdate)
        except Exception as ex:
            print("# MAKEDIRS ERROR: \n"+ str(ex), file=sys.stderr)
            p = subprocess.Popen(['mkdir', '-p', self.local_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
            res = p.communicate()[0]
            print(res)

        log_file=self.local_path+'{}/{}.log'.format(self.currdate, self.script_name)
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
    hive=sp.hc

    conn_schema = 'sbx_team_digitcamp'
    table_name = 'MA_CMDM_MA_DEAL_NEW'

    startdt = dt.now()

    #/////////////////////////////////////////////////////////////////////////////////////
    descr = hive.sql("describe extended {}.{}".format(conn_schema,table_name)).collect()
    hasPartitioned = len([item.asDict() for item in descr if item.asDict()['col_name'] =='# Partition Information']) > 0
    if hasPartitioned:
        try:
            parts = hive.sql("show partitions {}.{}".format(conn_schema,table_name)).collect()
            max_part = sorted(parts,reverse=True)[0]['partition']
            extract_date=re.compile("\d{4}\-\d{2}\-\d{2}")
            ext = extract_date.search(max_part)
            try:
                max_trunc_dt = ext.group(0)
            except:
                max_trunc_dt = None
        except (AnalysisException, IndexError):
            max_trunc_dt = None
    else:
        max_trunc_dt = None
    #/////////////////////////////////////////////////////////////////////////////////////


    if max_trunc_dt is not None:
      sql = '''select max(create_dt) from {}.{} where creation_month = '{}' '''.format(conn_schema,table_name,max_trunc_dt)
    else:
      sql = '''select max(create_dt) from {}.{}'''.format(conn_schema,table_name)

    max_dt = hive.sql(sql).collect()
    max_dt = max_dt[0]['max(create_dt)']
    max_resp_dt_str = dt.strftime(max_dt, format='%Y-%m-%d %H:%M:%S.%f')

    enddt = dt.now() - startdt

    _str = max_resp_dt_str #json.dumps(_dct, ensure_ascii=False, indent=3)

    sp.log("*"*120, sp.logger)
    sp.log("total calculation time: {}".format(enddt.total_seconds()), sp.logger)
    sp.log("max(create_dt): {}".format(_str), sp.logger)
    sp.log("*"*120, sp.logger)


