#!/opt/workspace/ektov1-av_ca-sbrf-ru/bin/python35
import os
import sys
curruser = os.environ.get('USER')

sys.path.insert(0, './src')
sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/libs/python3.5/site-packages/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata/lib/'.format(user=curruser))


import warnings
warnings.filterwarnings('ignore')

import logging
logging.basicConfig(filename='./logs/__aist_interaction_hist__.log',level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


import tendo.singleton
import joblib
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

from spark_connector import SparkConnector
from sparkdb_loader import spark
from connector import OracleDB
import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.dataframe import DataFrame

import re
import pandas as pd
import numpy as np
from tqdm._tqdm_notebook import tqdm_notebook
from pathlib import Path
import shutil
import loader as load


def print_and_log(message: str):
    print(message)
    logger.info(message)
    return None

sing = tendo.singleton.SingleInstance()

os.chdir('/opt/workspace/{}/notebooks/SOURCES_UPDATE/sourses/'.format(curruser))

print("### Starting spark context. Run!")

conn_schema = 'sbx_team_digitcamp' #'sbx_t_team_cvm'

sp = spark(schema=conn_schema,
           dynamic_alloc=False,
           numofinstances=15,
           numofcores=8,
           kerberos_auth=False,
           process_label="SAS_REPLICATION_"
           )
hive = sp.sql

print(sp.sc.version)

hive.setConf("hive.exec.dynamic.partition","true")
hive.setConf("hive.exec.dynamic.partition.mode","nonstrict")
hive.setConf("hive.enforce.bucketing","false")
hive.setConf("hive.enforce.sorting","false")


part_cols  =  [('interaction_start_month',),
               ('string',)
              ]
bucket_col = ["CALL_START_DATE", "CALL_END_DATE"]
bucket_num = 125

table_name = 'AIST_INTERACTION_HISTORY_NEW'

part_tupl_lst = ', '.join(["{} {}".format(col, _type) for col, _type in list(zip(part_cols[0],part_cols[1]))])
part_col_lst  = ', '.join([col for col in part_cols[0]])
if bucket_col is not None:
    bucket_cols = ', '.join([col for col in bucket_col])

print_and_log("### Getting new increment from Oracle snapshot of AIST_INTERACTION_HISTORY")

try:
    colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns
    # sdf = sdf.select(*colsinhive)
except AnalysisException:

    sdf = sp.get_oracle(OracleDB('iskra4'), '''( select /*+ parallel (4) */ * from ISKRA_CVM.AIST_INTERACTION_HISTORY)''')
    # strdate = datetime.strftime(datetime.now(), format='%Y.%d.%m')
    # currdate = datetime.strptime(strdate,'%Y.%d.%m')
    sdf = sdf.withColumn('interaction_start_month',f.trunc(f.to_date('interaction_start_date'),'MONTH').cast(StringType()))

    print_and_log("### Drop table {} if already exists".format(table_name))
    print_and_log("### Create new empty table {}".format(table_name))

    hive.sql("drop table if exists {schema}.{tbl} purge".format(schema=conn_schema, tbl=table_name))
    insert = ', '.join(["{} {}".format(col, _type) for col, _type in sdf.dtypes if col.lower() not in part_cols[0]])

    if bucket_col is not None:
        hive.sql('''create table {schema}.{tbl} (
                                                 {fields}
                                                    )
                     PARTITIONED BY ({part_col_lst})
                     CLUSTERED BY ({bucket_cols}) INTO {bucket_num} BUCKETS STORED AS PARQUET
                 '''.format(schema=conn_schema,
                            tbl=table_name,
                            fields=insert,
                            part_col_lst=part_tupl_lst,
                            bucket_num=bucket_num,
                            bucket_cols=bucket_cols)
                )
    else:
        hive.sql('''create table {schema}.{tbl} (
                                                 {fields}
                                                    )
                     PARTITIONED BY ({part_col_lst})
                 '''.format(schema=conn_schema,
                            tbl=table_name,
                            fields=insert,
                            part_col_lst=part_tupl_lst)
                )
    colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns

print_and_log("### Find last timestamp - max(interaction_start_date) in AIST_INTERACTION_HISTORY Hive datamart")

part_tbl = 'tmp_'+table_name

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
    sql = '''select max(interaction_start_date) from {}.{} where interaction_start_month = '{}' '''.format(conn_schema,table_name,max_trunc_dt)
else:
    sql = '''select max(interaction_start_date) from {}.{}'''.format(conn_schema,table_name)

max_dt = hive.sql(sql).collect()
print(max_dt[0]['max(interaction_start_date)'])
if (max_dt[0]['max(interaction_start_date)'] is not None):

    max_resp_dt_str = datetime.strftime(max_dt[0]['max(interaction_start_date)'], format='%Y-%m-%d %H:%M:%S.%f')
    print_and_log("### max(interaction_start_date) is {}".format(max_resp_dt_str))

    print_and_log("### Updating AIST_INTERACTION_HISTORY datamart. Perform Synchronization between Oracle and Hive dbs")
    print_and_log("### Selecting records from relevant Oracle table-->")

    if '.000000' in max_resp_dt_str:
      max_resp_dt_str = max_resp_dt_str.split('.000000')[0]
      sql = """
      (
      select /*+ parallel (4) */ * from ISKRA_CVM.AIST_INTERACTION_HISTORY
      where interaction_start_date > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss')
      )""".format(max_resp_dt_str)
      res = sp.get_oracle(OracleDB('iskra4'), sql)
      new_recs_cnt = res.count()

    elif '.' in max_resp_dt_str:
      sql = """
      (
      select /*+ parallel (4) */ * from ISKRA_CVM.AIST_INTERACTION_HISTORY
      where interaction_start_date > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss.ff6')
      )""".format(max_resp_dt_str)
      res = sp.get_oracle(OracleDB('iskra4'), sql)
      new_recs_cnt = res.count()

    res = res.withColumn('interaction_start_month',f.trunc(f.to_date('interaction_start_date'),'MONTH').cast(StringType()))

    colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns
    res = res.select(*colsinhive)
    res.registerTempTable(part_tbl)

else:

    colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns
    sdf = sdf.select(*colsinhive)
    sdf.registerTempTable(part_tbl)
    new_recs_cnt = sdf.count()

print_and_log("### Number of records to be inserted into Hive table: {}".format(new_recs_cnt))

if new_recs_cnt > 0:

    spStopCheck = sp.sc._jsc.sc().isStopped()
    if not spStopCheck:
        print("### Spark context is still alive!")
    else:
        sp = spark(schema=conn_schema,
                   dynamic_alloc=False,
                   numofinstances=15,
                   numofcores=8,
                   kerberos_auth=False,
                   process_label="SAS_REPLICATION_"
                   )
        hive = sp.sql

    if bucket_col is not None:
        hive.sql("""
        insert into table {schema}.{tbl}
        partition({part_col})
        select * from {tmp_tbl}
        cluster by ({bucket_cols})
        """.format(schema=conn_schema,
                   tbl=table_name,
                   tmp_tbl=part_tbl,
                   part_col=part_col_lst,
                   bucket_cols=bucket_cols)
                )
    else:
        hive.sql("""
        insert into table {schema}.{tbl}
        partition({part_col})
        select * from {tmp_tbl}
        distribute by ({part_col})
        """.format(schema=conn_schema,
                   tbl=table_name,
                   tmp_tbl=part_tbl,
                   part_col=part_col_lst)
                )
else:
    print_and_log("Nothing to insert. Bye!")

print_and_log("### Datamart {} has been updated succesfully".format(table_name))

print_and_log("Stoping Spark context. Done!")
sp.sc.stop()

sing.__del__()
