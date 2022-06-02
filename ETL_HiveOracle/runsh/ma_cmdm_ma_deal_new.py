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
logging.basicConfig(filename='./logs/__ma_cdmd_ma_deal__.log',level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


import tendo.singleton
import json
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
import decimal
import datetime
from getpass import getpass
from itertools import cycle

from spark_connector import SparkConnector
from sparkdb_loader import spark
from connector import OracleDB, TeraDB
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


#===============================================================================
WHICH_ETL_DRIVER = 'ORA' #'TERA' #'ORA'
TERADATA_HOST = "TDSB15"
DB = "PRD_DB_CLIENT4D_DEV1"
# DATABASE_NAME = ""
USERNAME = ""
PASSWORD = getpass()
db = TeraDB(TERADATA_HOST, DB, USERNAME, PASSWORD)
#===============================================================================


#*******************************************************************************
#*******************************************************************************
print("### Starting spark context. Run!")
conn_schema = 'sbx_team_digitcamp' #'sbx_t_team_cvm'
sp = spark(schema=conn_schema,
           dynamic_alloc=False,
           numofinstances=7,
           numofcores=8,
           kerberos_auth=False,
           process_label="SAS_REPLICATION_"
           )
hive = sp.sql
print(sp.sc.version)
#*******************************************************************************
#*******************************************************************************

#===============================================================================
#===============================================================================
hive.setConf("hive.input.dir.recursive","true")
hive.setConf("hive.supports.subdirectories","true")
hive.setConf("hive.mapred.supports.subdirectories","true")

hive.setConf("hive.exec.dynamic.partition","true")
hive.setConf("hive.exec.dynamic.partition.mode","nonstrict")
hive.setConf("hive.enforce.bucketing","false")
hive.setConf("hive.enforce.sorting","false")
# hive.setConf("hive.exec.stagingdir", "/tmp/{}/".format(curruser))
# hive.setConf("hive.exec.scratchdir", "/tmp/{}/".format(curruser))
hive.setConf("hive.load.dynamic.partitions.thread", 10)
hive.setConf('hive.metastore.fshandler.threads',15)
hive.setConf('hive.msck.repair.batch.size', 1000)
hive.setConf('hive.merge.smallfiles.avgsize',256000000)
hive.setConf('hive.merge.size.per.task',256000000)
#===============================================================================
#===============================================================================

part_cols  =  [('creation_month', 'product_group'),
               ('string', 'string')
              ]
bucket_col = ["complete_dt", "inn"]
bucket_num = 125

# set the fix number of sorted patitions to catch from metastore
NUMDAYS = 10

part_tupl_lst = ', '.join(
                              ["{} {}".format(col, _type) for col, _type in
                                       list(zip(part_cols[0],part_cols[1]))
                              ]
                          )

part_col_lst  = ', '.join([col for col in part_cols[0]])

if bucket_col is not None:
    bucket_cols = ', '.join([col for col in bucket_col])

#===============================================================================
table_name = 'MA_CMDM_MA_DEAL_NEW'
prod_dict_tbl = 'ma_dict_v_product_dict'
#===============================================================================

fPathtoLastDtDict = './json/saveLastDt'

with open(fPathtoLastDtDict,'r') as fobj:
    dbsLastDt = json.load(fobj)

maxLastDt = dbsLastDt[table_name]
#===============================================================================

prod_sdf = hive.sql('''select
                              crm_product_id,
                              crm_active_flg,
                              product_full_nm,
                              product_short_nm,
                              product_cd_mmb,
                              product_group,
                              product_subgroup,
                              product_type,
                              product_priority,
                              channel_sale,
                              channel_eco_expert,
                              channel_mmb_ms,
                              channel_mmb_ckr,
                              channel_mmb_mkk,
                              channel_dzo
                        from {}.{}'''.format(conn_schema, prod_dict_tbl))


try:
    colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns
    # sdf = sdf.select(*colsinhive)
except AnalysisException:
    if WHICH_ETL_DRIVER == 'ORA':
        print_and_log("### Getting new increment from Oracle snapshot of MA_CMDM_MA_DEAL")
        deals = sp.get_oracle(OracleDB('iskra4'), '''(select /*+ parallel (4) */ * from ISKRA_CVM.MA_CMDM_MA_DEAL)''')
    else:
        print_and_log("### Getting new increment from Teradata snapshot of MA_CMDM_MA_DEAL")
        db = TeraDB(TERADATA_HOST, DB, USERNAME, PASSWORD)
        deals = sp.get_teradata(db, '''(select * from {}.MA_CMDM_MA_DEAL) as t'''.format(DB))
    # strdate = datetime.strftime(datetime.now(), format='%Y.%d.%m')
    # currdate = datetime.strptime(strdate,'%Y.%d.%m')
    deals = deals.withColumn('creation_month', f.trunc(f.to_date('create_dt'),'MONTH').cast(StringType()))

    conditions = (deals.HOST_PROD_ID == prod_sdf.crm_product_id)
    sdf = deals.join(prod_sdf, how='left_outer', on=conditions)
    sdf = sdf.drop('crm_product_id')

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


print_and_log("### Find last timestamp - max(create_dt) in MA_CMDM_MA_DEAL Hive datamart")

part_tbl = 'tmp_'+table_name


#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
descr = hive.sql("describe extended {}.{}".format(conn_schema,table_name)).collect()
push_down = True
loop_rows = cycle(descr)
nextelem = next(loop_rows)
while push_down:
    thiselemen, nextelem = nextelem, next(loop_rows)
    if nextelem.asDict()['col_name'] =='# Partition Information':
        next(loop_rows)
        part_col = next(loop_rows)
        part_col_info = part_col.asDict()
        push_down = False

#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
part_name = part_col_info['col_name']
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

hasPartitioned = len([item.asDict() for item in descr if item.asDict()['col_name'] =='# Partition Information']) > 0
if hasPartitioned:
    try:
        parts = hive.sql("show partitions {}.{}".format(conn_schema,table_name)).collect()
        parts = [part['partition'] for part in parts if not part['partition'].endswith('__HIVE_DEFAULT_PARTITION__')]
        parts = sorted(parts,reverse=True)
        max_part = parts[0]
        extract_date=re.compile("\d{4}\-\d{2}\-\d{2}")
        ext = extract_date.search(max_part)
        try:
            max_trunc_dt = ext.group(0)
        except:
            max_trunc_dt = None
        if part_col_info['data_type'] in ('date', 'timestamp', 'string') and (max_trunc_dt is not None):
            parts = sorted(parts, reverse=True, key=lambda x:
                           datetime.datetime.strptime(x.split(part_name+'=')[-1].split('/')[0], '%Y-%m-%d'))
        else:
            parts = sorted(parts, reverse=True, key=lambda x: int(x.split(part_name+'=')[-1].split('/')[0]))
        last_part_lst = sorted(list(set([part.split(part_name+'=')[-1].split('/')[0] for part in parts])),
                               reverse=True)[:NUMDAYS]
    except (AnalysisException, IndexError):
        last_part_lst = None
else:
    last_part_lst = None
#/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

if maxLastDt == None:

  if max_trunc_dt is not None:
      sql = '''select max(create_dt) from {}.{} where creation_month = '{}' '''.format(conn_schema,
                                                                                       table_name,max_trunc_dt
                                                                                       )
  else:
      sql = '''select max(create_dt) from {}.{}'''.format(conn_schema, table_name)

  max_dt = hive.sql(sql).collect()
  max_dt = max_dt[0]['max(create_dt)']

else:
  max_dt = maxLastDt

if (max_dt is not None):
    if not isinstance(max_dt, str):
      max_resp_dt_str = datetime.strftime(max_dt, format='%Y-%m-%d %H:%M:%S.%f')
    else:
      max_resp_dt_str = max_dt
    print_and_log("### max(create_dt) is {}".format(max_resp_dt_str))

    print_and_log("### Updating MA_CMDM_MA_DEAL datamart. Perform Synchronization between Oracle and Hive dbs")
    print_and_log("### Selecting records from relevant Oracle table-->")
    if WHICH_ETL_DRIVER == 'ORA':
        if '.000000' in max_resp_dt_str:
          max_resp_dt_str = max_resp_dt_str.split('.000000')[0]
          sql = """
          (
          select /*+ parallel (4) */ * from ISKRA_CVM.MA_CMDM_MA_DEAL
          where create_dt > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss')
          )""".format(max_resp_dt_str)
          res = sp.get_oracle(OracleDB('iskra4'), sql)
          new_recs_cnt = res.count()

        elif '.' in max_resp_dt_str:
          sql = """
          (
          select /*+ parallel (4) */ * from ISKRA_CVM.MA_CMDM_MA_DEAL
          where create_dt > to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss.ff6')
          )""".format(max_resp_dt_str)
          res = sp.get_oracle(OracleDB('iskra4'), sql)
          new_recs_cnt = res.count()
    else:
        if '.000000' in max_resp_dt_str:
          max_resp_dt_str = max_resp_dt_str.split('.000000')[0]
          sql = """
          (
          select * from {}.MA_CMDM_MA_DEAL
          where create_dt > to_timestamp('{}', 'YYYY-MM-DD HH24:MI:SS')
          ) as t""".format(DB, max_resp_dt_str)
          res = sp.get_teradata(db, sql)
          new_recs_cnt = res.count()

        elif '.' in max_resp_dt_str:
          sql = """
          (
          select * from {}.MA_CMDM_MA_DEAL
          where create_dt > to_timestamp('{}', 'YYYY-MM-DD HH24:MI:SS.FF6')
          ) as t""".format(DB, max_resp_dt_str)
          res = sp.get_teradata(db, sql)
          new_recs_cnt = res.count()

    res = res.withColumn('creation_month',f.trunc(f.to_date('create_dt'),'MONTH').cast(StringType()))

    max_dt = res.select(f.max(f.col("create_dt")).alias("max_create_dt")).collect()
    max_dt = max_dt[0]['max_create_dt']
    if (max_dt is not None):
        max_resp_dt_str = datetime.strftime(max_dt, format='%Y-%m-%d %H:%M:%S.%f')
        print_and_log("### Oracle new max(create_dt) is {}".format(max_resp_dt_str))
    else:
        max_resp_dt_str = maxLastDt

    if WHICH_ETL_DRIVER == 'ORA':
        conditions = (res.HOST_PROD_ID == prod_sdf.crm_product_id)
        sdf = res.join(prod_sdf, how='left_outer', on=conditions)
        sdf = sdf.drop('crm_product_id')
    else:
        print_and_log("### TERA: DataBase already enriched by Product Dict data")
        sdf = res

    colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns
    sdf = sdf.select(*colsinhive)
    sdf.registerTempTable(part_tbl)

else:

    colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns
    sdf = sdf.select(*colsinhive)
    sdf.registerTempTable(part_tbl)
    new_recs_cnt = sdf.count()

    max_dt = sdf.select(f.max(f.col("create_dt")).alias("max_create_dt")).collect()
    max_dt = max_dt[0]['max_create_dt']
    if (max_dt is not None):
        max_resp_dt_str = datetime.strftime(max_dt, format='%Y-%m-%d %H:%M:%S.%f')
        print_and_log("### Oracle new max(create_dt) is {}".format(max_resp_dt_str))
    else:
        max_resp_dt_str = maxLastDt

print_and_log("### Number of records to be inserted into Hive table: {}".format(new_recs_cnt))

if new_recs_cnt > 0:

    spStopCheck = sp.sc._jsc.sc().isStopped()
    if not spStopCheck:
        print("### Spark context is still alive!")
    else:
        sp = spark(schema=conn_schema,
                   dynamic_alloc=False,
                   numofinstances=10,
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
        # sp.insert_to_hive(sdf, out_table='{0}.{1}'.format(conn_schema,table_name),
        #                       mode = 'append',
        #                       frmt = 'hive',
        #                       partitionCols = None, #part_cols[0],
        #                       bucketCols = [f.col(_col) for _col in bucket_col],
        #                       bucketNum = bucket_num)
    else:
        pass
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

    dbsLastDt[table_name] = max_resp_dt_str
    with open(fPathtoLastDtDict,'w') as fobj:
        json.dump(dbsLastDt, fobj, indent=4, sort_keys=True)

else:
    print_and_log("Nothing to insert. Bye!")

print_and_log("### Datamart {} has been updated succesfully".format(table_name))

print_and_log("Stoping Spark context. Done!")
sp.sc.stop()

sing.__del__()
