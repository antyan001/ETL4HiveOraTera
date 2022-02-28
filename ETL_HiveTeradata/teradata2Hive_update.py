#!/home/ektov1-av_ca-sbrf-ru/bin/python35
import os
import sys

curruser = os.environ.get('USER')

sys.path.insert(0, './src')
sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/libs/python3.5/site-packages/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata/lib/'.format(user=curruser))


import tendo.singleton

import warnings
warnings.filterwarnings('ignore')

import logging
logging.basicConfig(filename='./_teradata2Hive_update_.log',level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

import jaydebeapi

from pathlib import Path
from datetime import datetime
from etl import ETLtera

from spark_connector import SparkConnector
from sparkdb_loader import spark
from time import sleep
from connector import OracleDB
import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame

import re
import pandas as pd
import numpy as np

sing = tendo.singleton.SingleInstance()

os.chdir('/opt/workspace/{user}/notebooks/TERADATA_UPD/'.format(user=curruser))

os.environ['PATH'] ='''PATH=/usr/local/bin:/usr/bin:
                            /home/$USER/bin:/opt/python/virtualenv/jupyter/lib/node_modules/.bin:
                            /opt/python/virtualenv/jupyter/bin:/opt/python/virtualenv/jupyter/bin:
                            /sbin:/bin:/usr/sbin:/usr/bin:/opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bin:
                            /opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/sbin:
                            /opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/usr/bin:
                            /opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bigartm/bin:
                            /opt/workspace/$USER/notebooks/drivers/:/usr/java/jdk1.8.0_131/jre/bin'''


etl = ETLtera()

print("### Starting spark context. Run!")

#conn_schema= 'external_clickstream'
conn_schema= 'sbx_team_digitcamp'
sp = spark(schema=conn_schema,
           sparkVersion='2.2',
           dynamic_alloc=False)
print(sp.sc.version)
hive = sp.sql


cnt = hive.sql("select * from {schema}.{tbl}".format(schema=conn_schema,tbl='INTERNAL_CLICKSTREAM_SITE_CORP')).count()
print(cnt)

print('### Create connect to Teradata server...')

TERADATA_HOST = "TDSB15.cgs.sbrf.ru"
# DATABASE_NAME = "SBX_DB_SME_DKK_ANALYSE"
USERNAME = "Ektov-AV"
PASSWORD = 'eSpewvRkHBG31yd'
JDBC_ARGUMENTS = "CHARSET=UTF8,TMODE=ANSI"
# JDBC_ARGUMENTS = "CHARSET=UTF8,TMODE=ANSI,DATABASE={}".format(DATABASE_NAME)

print("#"*120)
print('## Update Hive tables using timestamp references')
print("#"*120)

#tbls = [('PRD_VD_CLIENT4D_SEGM.CLICKSTREAM_SITE_CORP', 'CLICKSTREAM_SITE_CORP'),
#        ('PRD_VD_CLIENT4D_SEGM.CLICKSTREAM_COOKIE_DCCVMID', 'CLICKSTREAM_COOKIE_DCCVMID')]

tbls = [('PRD_DB_CLIENT4D_SEGM.CLICKSTREAM_SITE_CORP', 'INTERNAL_CLICKSTREAM_SITE_CORP')]

for items in tbls:

    teradata_name, table_name = items

    print("*"*120)
    print('Working with {} table'.format(table_name))
    print("*"*120)

    logger.info("### Working with {} table".format(table_name))

    my_sql = "SELECT top 50000 * FROM {}".format(teradata_name)

    conn = jaydebeapi.connect(
    jclassname="com.teradata.jdbc.TeraDriver",
    url="jdbc:teradata://{}/{}".format(TERADATA_HOST, JDBC_ARGUMENTS),
    driver_args={"user": USERNAME, "password": PASSWORD},
    jars=['/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/drivers/tdgssconfig.jar',
          '/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/drivers/terajdbc4.jar']
    )

    curs = conn.cursor()
    curs.execute(my_sql)
    cols = [col[0] for col in curs.description]
    df = etl.get_df_from_teradata(curs, cols, size=50000)
    curs.close()
    conn.close()

    typesmap_rdd={}
    typesmap_pd={}

    for column_name, column in df.iteritems():
        if column.dtype.kind == 'O':
            typesmap_rdd[column_name] = StringType()
            typesmap_pd[column_name]  = str
        elif column.dtype.kind == 'i':
            typesmap_rdd[column_name] = IntegerType()
            typesmap_pd[column_name]  = np.int
        elif column.dtype.kind == 'M':
            typesmap_rdd[column_name] = LongType()
            typesmap_pd[column_name]  = np.datetime64
        elif column.dtype.kind == 'f':
            typesmap_rdd[column_name] = FloatType()
            typesmap_pd[column_name]  = np.float
        else:
            None

    print('## Fetch batches from Teradata and push them to Hive')

    print('### Select max `hit_date` from Hive table')
    logger.info("### Select max `hit_date` from Hive table {}".format(table_name))

    sql = '''select max(hit_date) from {}.{}'''.format(conn_schema, table_name)
    max_dt = hive.sql(sql).collect()
    max_resp_dt_str = max_dt[0]['max(hit_date)']

    print(max_resp_dt_str)
    logger.info("### max `hit_date` is {}".format(max_resp_dt_str))

    print('### Sampling relevant data from Teradata table according to the date filtering rule')

    my_sql = "SELECT * FROM {} where hit_date > to_date('{}', 'yyyy-mm-dd hh24:mi:ss')".format(teradata_name, max_resp_dt_str)

    conn = jaydebeapi.connect(
    jclassname="com.teradata.jdbc.TeraDriver",
    url="jdbc:teradata://{}/{}".format(TERADATA_HOST, JDBC_ARGUMENTS),
    driver_args={"user": USERNAME, "password": PASSWORD},
    jars=['/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/drivers/tdgssconfig.jar',
          '/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/terajdbc4.jar']
    )
    curs = conn.cursor()
    curs.execute(my_sql)

    print('### Update Hive table with new buckets selected from Teradata')

    cols = [col[0] for col in curs.description]
    while True:
        df = etl.get_df_from_teradata(curs, cols, size=500000)
    #     df = df.astype(str)
        if df.shape[0]!=0:
            spStopCheck = sp.sc._jsc.sc().isStopped()
            if not spStopCheck:
                print("### Spark context is still alive!")
            else:
                sp = spark(schema=conn_schema)
                hive = sp.sql
            etl.update_table_hive(sp, df, typesmap_rdd, typesmap_pd, conn_schema, table_name)
            print('### {} records has been inserted...'.format(df.shape[0]))
            logger.info("### {} records has been inserted...".format(df.shape[0]))
        else:
            print("Nothing to insert. Bye!")
            logger.info("### {} records has been inserted...".format(df.shape[0]))
            break

    curs.close()
    conn.close()

print("#"*120)
print("### Update Hive tables via drop/reload")
print("#"*120)

# tbls = [('PRD_VD_CLIENT4D_SEGM.CLICKSTREAM_INN_COOKIE', 'CLICKSTREAM_INN_COOKIE'),
#         ('PRD_VD_CLIENT4D_SEGM.CLICKSTREAM_DCCVMID_INN_DICT', 'CLICKSTREAM_DCCVMID_INN_DICT')]
tbls = [('PRD_VD_CLIENT4D_SEGM.CLICKSTREAM_INN_COOKIE', 'INTERNAL_CLICKSTREAM_INN_COOKIE')]

for items in tbls:

    teradata_name, table_name = items

    print("*"*120)
    print("### Working with table {}".format(table_name))
    print("*"*120)
    logger.info("### Working with {} table".format(table_name))

    print('### Create Hive table')

    my_sql = "SELECT * FROM {} sample 10".format(teradata_name)
    typesmap_rdd, typesmap_pd = etl.create_table_hive(sp, my_sql, conn_schema, table_name)

    print('### Selecting records from teradata')

    my_sql = "SELECT * FROM {}".format(teradata_name)

    conn = jaydebeapi.connect(
    jclassname="com.teradata.jdbc.TeraDriver",
    url="jdbc:teradata://{}/{}".format(TERADATA_HOST, JDBC_ARGUMENTS),
    driver_args={"user": USERNAME, "password": PASSWORD},
    jars=['/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/drivers/tdgssconfig.jar',
          '/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/terajdbc4.jar']
    )
    curs = conn.cursor()
    curs.execute(my_sql)

    print('### Populate Hive table with buckets selected from Teradata')

    cols = [col[0] for col in curs.description]
    while True:
        df = etl.get_df_from_teradata(curs, cols, size=500000)
    #     df = df.astype(str)
        if df.shape[0]!=0:
            spStopCheck = sp.sc._jsc.sc().isStopped()
            if not spStopCheck:
                print("### Spark context is still alive!")
            else:
                sp = spark(schema=conn_schema)
                hive = sp.sql
            etl.update_table_hive(sp, df, typesmap_rdd, typesmap_pd, conn_schema, table_name)
            print('### {} records has been inserted...'.format(df.shape[0]))
            logger.info("### {} records has been inserted...".format(df.shape[0]))
        else:
            print("Nothing to insert. Bye!")
            logger.info("### {} records has been inserted...".format(df.shape[0]))
            break

    curs.close()
    conn.close()

    print("*"*120)


print("Stoping Spark context. Done!")
sp.sc.stop()

sing.__del__()
