#!/home/ektov1-av_ca-sbrf-ru/bin/python35
import os
import sys
curruser = os.environ.get('USER')

# sys.path.insert(0, '/opt/workspace/{user}/system/support_library/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/libs/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/system/labdata/lib/'.format(user=curruser))


sys.path.insert(0, './../src')
sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/libs/python3.5/site-packages/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata/lib/'.format(user=curruser))

import warnings
warnings.filterwarnings('ignore')

import loader as load
import pandas as pd
import jaydebeapi
import re
from pathlib import Path

from spark_connector import SparkConnector
from sparkdb_loader import spark
from time import sleep

import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame

import re
import pandas as pd
import numpy as np
from datetime import datetime

os.environ['PATH'] = "/usr/local/bin:/usr/bin:/home/$USER/bin:/opt/python/virtualenv/jupyter/lib/node_modules/.bin:/opt/python/virtualenv/jupyter/bin:/opt/python/virtualenv/jupyter/bin:/sbin:/bin:/usr/sbin:/usr/bin:/opt/cloudera/parcels/PYENV.ZNO0059623792/bin:/opt/cloudera/parcels/PYENV.ZNO0059623792/sbin:/opt/cloudera/parcels/PYENV.ZNO0059623792/usr/bin:/opt/cloudera/parcels/PYENV.ZNO0059623792/bigartm/bin:\
/home/$USER/notebooks/drivers/:/usr/java/jdk1.8.0_131/jre/bin"

class ETLORA(object):

    def __init__(self):
        self.USERNAME = ""
        self.PASSWORD = ''


    def get_df_from_oracle(self, curs, cols, size=100):
        raw_data = curs.fetchmany(size)
        if raw_data is not None or len(raw_data)!=0:
            df = pd.DataFrame(raw_data, columns = cols)
    #         rowcount = curs.rowcount
        else:
            df =  None
        return df

    def get_data_from_oracle(self, curs, my_sql: str) -> list:

        # curs = conn.cursor()
        curs.execute(my_sql)
        raw_data = curs.fetchall()
        columns = [col[0] for col in curs.description]
        # curs.close()
        return raw_data, columns


    def create_table_hive(self, curs, sp, my_sql, schema, table_name):

        # curs = conn.cursor()
        curs.execute(my_sql)
        cols = [col[0] for col in curs.description]
        raw_data = curs.fetchall()
        if raw_data is not None or len(raw_data)!=0:
            df = pd.DataFrame(raw_data, columns = cols)

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
                typesmap_rdd[column_name] = TimestampType()
                typesmap_pd[column_name]  = np.datetime64
            elif column.dtype.kind == 'f':
                typesmap_rdd[column_name] = FloatType()
                typesmap_pd[column_name]  = np.float
            else:
                None

        cols = list(df.columns.values)
        while True:
            try:
                spdf = sp.sql.createDataFrame(df, schema=StructType([StructField(col, typesmap_rdd[col]) for col in df.columns]))
            except TypeError as ex:
                if 'LongType can not accept object' in str(ex):
                    datecols = [k for k,v in typesmap_pd.items() if np.issubdtype(v,np.datetime64)]
                    for col in datecols:
                        typesmap_rdd[col] = TimestampType()
            except ValueError as ex:
                    if 'object of IntegerType out of range' in str(ex):
                        errstr = str(ex)
                        try:
                            col = errstr.split(':')[0].split(' ')[1]
                            typesmap_rdd[col] = LongType()
                        except:
                            intcols = [k for k,v in typesmap_pd.items() if np.issubdtype(v,np.int64)]
                            for col in intcols:
                                typesmap_rdd[col] = LongType()
            finally:
                try:
                    spdf = sp.sql.createDataFrame(df, schema=StructType([StructField(col, typesmap_rdd[col]) for col in df.columns]))
                    break
                except:
                    pass

        try:
            spdf = sp.sql.createDataFrame(df, schema=StructType([StructField(col, typesmap_rdd[col]) for col in df.columns]))
        except ValueError as err:
            errstr = str(err)
            if "object of IntegerType out of range" in errstr:
                col = errstr.split(':')[0].split(' ')[1]
                typesmap_rdd[col] = LongType()

            spdf = sp.sql.createDataFrame(df, schema=StructType([StructField(col, typesmap_rdd[col]) for col in df.columns]))

        for column_name, _type in spdf.dtypes:
            if _type == 'bigint' and (typesmap_pd[column_name]!=np.int):
                tmp = spdf.select(column_name).take(1)
                if (len(str(tmp[0][column_name])) > 10)&(len(str(tmp[0][column_name])) <= 13):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000).cast(TimestampType()))
                elif (len(str(tmp[0][column_name])) > 13)&(len(str(tmp[0][column_name])) <= 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000).cast(TimestampType()))
                elif (len(str(tmp[0][column_name])) > 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000000).cast(TimestampType()))
                else:
                  spdf = spdf.withColumn('Date', (f.col(column_name)).cast(TimestampType()))
                spdf = spdf.drop(column_name)
                spdf = spdf.withColumnRenamed('Date', column_name)

        spdf = spdf.limit(0)
        spdf.registerTempTable(table_name)
        sp.sql.sql("DROP TABLE IF EXISTS {db}.{tab}".format(db=schema, tab=table_name))
        sql_cr = 'CREATE TABLE {db}.{tab} SELECT * FROM {tab}'.format(db=schema, tab=table_name)
        sp.sql.sql(sql_cr)

        return typesmap_rdd, typesmap_pd


    def update_table_hive(self, sp, df, typesmap_rdd, typesmap_pd, schema, table_name):

        def datetime2timestamp(x):
            if x is not None and isinstance(x, pd.datetime):
                if x.year < 2020:
                    return pd.to_datetime(x)
                else:
                    return pd.to_datetime(datetime(2199, 1, 1, 0, 0))
            else:
                return pd.to_datetime(pd.NaT)

        for col in df.columns:
            if df[col].dtype.kind == 'i' or df[col].dtype.kind == 'f':
                df[col] = df[col].fillna(0)
                df[col] = df[col].astype(typesmap_pd[col])
            elif df[col].dtype.kind == 'M':
                pass
                # cast to timestamps.Timestamp from datetime.datetime
                # df[col] = df[col].apply( lambda x: pd.to_datetime(x))
            elif ( (df[col].dtype.kind == 'O') and
                   (typesmap_pd[col] != str) and
                   (typesmap_pd[col] == np.datetime64) ):
                df[col] = df[col].apply(lambda x: datetime2timestamp(x))

        spdf = sp.sql.createDataFrame(df, schema=StructType([StructField(col, typesmap_rdd[col]) for col in df.columns]))

        for column_name, _type in spdf.dtypes:
            if _type == 'bigint' and (typesmap_pd[column_name]!=np.int):
                tmp = spdf.select(column_name).take(1)
                if (len(str(tmp[0][column_name])) > 10)&(len(str(tmp[0][column_name])) <= 13):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000).cast(TimestampType()))
                elif (len(str(tmp[0][column_name])) > 13)&(len(str(tmp[0][column_name])) <= 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000).cast(TimestampType()))
                elif (len(str(tmp[0][column_name])) > 16):
                  spdf = spdf.withColumn('Date', (f.col(column_name) / 1000000000).cast(TimestampType()))
                else:
                  spdf = spdf.withColumn('Date', (f.col(column_name)).cast(TimestampType()))
                spdf = spdf.drop(column_name)
                spdf = spdf.withColumnRenamed('Date', column_name)

        spdf.registerTempTable('tmp_'+table_name)
        sp.sql.sql('insert into table {}.{} select * from {}'.format(schema, table_name,'tmp_'+table_name))

        del spdf



