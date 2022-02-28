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
logging.basicConfig(filename='./logs/__sme_cdm_offer_proc_coe__.log',level=logging.INFO,
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
from py4j.protocol import Py4JJavaError
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

conn_schema = 'sbx_t_team_cvm'


tbls_lst = \
[
  'MA_DICT_V_PRODUCT_DICT',
  'MA_MMB_A1_60_INCOME_PROD_ALL',
  'MA_DICT_MA_MMB_CONV_COEF_DICT',
  'SME_CDM_MA_MMB_ALL_TB_COEF'
]

sp = spark(schema=conn_schema,
           dynamic_alloc=False,
           numofinstances=10,
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

# part_cols  =  [('load_dttm', 'sources_lv2_list'),
#                ('string', 'string')
#               ]
# bucket_col = ["inn"]
# bucket_num = 125


for table_name in tbls_lst:

    # table_name = 'MMB_OFFER_PROC_COE'

    fPathtoLastDtDict = './json/saveLastDt'

    with open(fPathtoLastDtDict,'r') as fobj:
        dbsLastDt = json.load(fobj)

    maxLastDt = dbsLastDt.get(table_name, None)

    # part_tupl_lst = ', '.join(["{} {}".format(col, _type) for col, _type in list(zip(part_cols[0],part_cols[1]))])
    # part_col_lst  = ', '.join([col for col in part_cols[0]])
    # bucket_cols = ', '.join([col for col in bucket_col])


    print_and_log("### Getting new increment from Oracle snapshot of MA_TMP_MMB_OFFER_PROC_COE")
    try:
      sdf = sp.get_oracle(OracleDB('iskra4'), '''(select /*+ parallel (4) */ * from ISKRA_CVM.{})'''.format(table_name))
    except Py4JJavaError:
      print_and_log("### ORA-00942: table or view does not exist")
      sp.sc.stop()
      sys.exit(0)

    strdate = datetime.strftime(datetime.now(), format='%Y.%d.%m')
    # currdate = datetime.strptime(strdate,'%Y.%d.%m')
    sdf = sdf.withColumn('load_dttm',f.lit(strdate))

    try:
        colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns
        # sdf = sdf.select(*colsinhive)
    except AnalysisException:
        print_and_log("### Drop table {} if already exists".format(table_name))
        print_and_log("### Create new empty table {}".format(table_name))

        hive.sql("drop table if exists {schema}.{tbl} purge".format(schema=conn_schema, tbl=table_name))
        insert = ', '.join(["{} {}".format(col, _type) for col, _type in sdf.dtypes])

        hive.sql('''create table {schema}.{tbl} (
                                                 {fields}
                                                    )
                 '''.format(schema=conn_schema,
                            tbl=table_name,
                            fields=insert)
                )

        colsinhive = hive.sql("select * from {}.{}".format(conn_schema,table_name)).columns


    part_tbl = 'tmp_{}'.format(table_name)

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
                       numofinstances=10,
                       numofcores=8,
                       kerberos_auth=False,
                       process_label="SAS_REPLICATION_"
                       )
            hive = sp.sql

        hive.sql("""
        insert into table {schema}.{tbl}
        select * from {tmp_tbl}
        """.format(schema=conn_schema,
                   tbl=table_name,
                   tmp_tbl=part_tbl)
                )

        dbsLastDt[table_name] = strdate
        with open(fPathtoLastDtDict,'w') as fobj:
            json.dump(dbsLastDt, fobj, indent=4, sort_keys=True)

    else:
        print_and_log("Nothing to insert. Bye!")

    print_and_log("### Datamart {} has been updated succesfully".format(table_name))

print_and_log("Stoping Spark context. Done!")
sp.sc.stop()

sing.__del__()
