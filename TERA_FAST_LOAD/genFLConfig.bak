#!/opt/workspace/ektov1-av_ca-sbrf-ru/bin/python35

import os
import sys
curruser = os.environ.get('USER')

# sys.path.insert(0, '/opt/workspace/{user}/system/support_library/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/libs/'.format(user=curruser))
# sys.path.insert(0, '/opt/workspace/{user}/system/labdata/lib/'.format(user=curruser))


# sys.path.insert(0, './../src')
sys.path.insert(0, '/opt/workspace/{user}/notebooks/support_library/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/libs/python3.5/site-packages/'.format(user=curruser))
sys.path.insert(0, '/opt/workspace/{user}/notebooks/labdata/lib/'.format(user=curruser))

#import tendo.singleton
import warnings
warnings.filterwarnings('ignore')

import logging
logging.basicConfig(filename='./__tera_fload__.log',level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


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
import decimal
import datetime
from getpass import getpass

import jaydebeapi

from transliterate import translit

from spark_connector import SparkConnector
from sparkdb_loader import spark
from connector import OracleDB, TeraDB
import pyspark
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException

from pyspark.sql import Row

import re
import pandas as pd
import numpy as np
from tqdm import tqdm
from pathlib import Path
import shutil
import loader as load
import pandas as pd
import csv
from itertools import cycle

# sing = tendo.singleton.SingleInstance()

# os.chdir('/opt/workspace/ektov1-av_ca-sbrf-ru/notebooks/Clickstream_Analytics/AutoUpdate/')
# os.chdir('/opt/workspace/{}/notebooks/clickstream/AutoUpdate/'.format(curruser))


##**************************************************************************************************
##**************************************************************************************************
tmp_tbl_name = "tmp_hive2tera_cast"
EXPORT_PATH = "/opt/workspace/{curruser}/notebooks/TERA_FAST_LOAD/csv".format(curruser = os.environ.get('USER'))
CONF_OUT_PATH__ = "/opt/workspace/{curruser}/notebooks/TERA_FAST_LOAD/run.fld".format(curruser = os.environ.get('USER'))

conn_schema = 'sbx_t_team_cvm' #'sbx_team_digitcamp' #'sbx_t_team_cvm'
table_name = 'lal_db_hist_in' #'ma_cmdm_ma_deal_new' #'lal_db_hist_in'

# set the fix number of sorted patitions to catch from metastore
numdays = 15
BATCH_SPLIT__ = 2

TERADATA_HOST = "TDSB15.cgs.sbrf.ru"
DB = "PRD_DB_CLIENT4D_DEV1"
USERNAME = "ektov1-av"
##**************************************************************************************************
##**************************************************************************************************

def show(self, n=10):
    return self.limit(n).toPandas()

def typed_udf(return_type):
    '''Make a UDF decorator with the given return type'''

    def _typed_udf_wrapper(func):
        return f.udf(func,return_type)

def essense(channel: str, prod_cd: str):
    message = "{}: {} retargeting".format(channel, prod_cd)
    return message

essense_udf = f.udf(essense, StringType())

pyspark.sql.dataframe.DataFrame.show = show

def print_and_log(message: str):
    print(message)
    logger.info(message)
    return None

def log(message, logger,
        print_log : bool = True):
    if not print_log:
        return

    logger.info(message)
    print(message, file=sys.stdout)
    # print(message, file=sys.stderr)


def block_iterator(iterator,size):
    """
    dest:
        сервисная функция для итерации по блокам данных внутри RDD.
        Чем больше size при увеличении количества потоков, тем быстрее обработка
    args:
        iterator-объект
        size - размер элементов для единичной итерации
    return:
        вычисляемый объект bucket

    """
    bucket = list()
    for e in iterator:
        bucket.append(e)
        if len(bucket) >= size:
            yield bucket
            bucket = list()
    if bucket:
        yield bucket

def block_classify(iterator):
    import os
    import sys
    import pandas as pd
    import json

    for out in block_iterator(iterator, 100):

        cols = [col for col,_ in col_bc.value]
        currschema = StructType([StructField(col, typesmap_rdd_bc.value[col]) for col in cols])
        sdf_proc = sp.sql.createDataFrame(out.collect(), schema=currschema)
        break

    return sdf_proc

def collectRowsByIndex(i, it, indxs):
    out = []
    if i in indxs:
         out.extend(list(it)) #islice(it,0,5)
    else:
        pass

    return out



if __name__ == "__main__":

    print_and_log("### Starting spark context. Run!")

    sp = spark(schema=conn_schema,
               dynamic_alloc=False,
               kerberos_auth=False,
               numofinstances=8,
               numofcores=8,
               executor_memory='30g',
               driver_memory='30g'
               )
    hive = sp.sql

    print_and_log(sp.sc.version)


    print_and_log("## Set a Connection to Teradata")
    PASSWORD = getpass()
    db = TeraDB(TERADATA_HOST, DB, USERNAME, PASSWORD)

    print_and_log("## Get actual partitions from MetaStore")

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


    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    part_name = part_col_info['col_name']
    # set the fix number of sorted patitions to catch from metastore
    # numdays = 15
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
                                   reverse=True)[:numdays]
        except (AnalysisException, IndexError):
            last_part_lst = None
    else:
        last_part_lst = None
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    print_and_log("## Filter Corrupted Partitions")

    HDFS_PATH = "hdfs://clsklsbx/user/team/{}/hive/{}/{}".format(conn_schema.split("sbx_")[-1].split("t_")[-1],
                                                                 table_name,
                                                                 part_name)+"={}/"
    noncorrupCTL = []
    for ctl in last_part_lst:
        try:
            sdf = hive.read.load(path=HDFS_PATH.format(ctl), format='parquet').limit(10)
            noncorrupCTL.append(ctl)
        except (AnalysisException) as err:
            if "Unable to infer schema for Parquet" in str(err):
                print(" Unable to infer schema for Parquet. It must be specified manually")


    print_and_log("## Select Appropriate Rows Under User Conditions")

    sdf = hive.sql('''select * from {}.{}
                      where {} between '{}' and '{}' '''.format(conn_schema,
                                                                table_name,
                                                                part_name.lower(),
                                                                noncorrupCTL[-1],
                                                                noncorrupCTL[0]
                                                               )
                  )

    sdf = sdf.withColumn("ROW_ID", f.monotonically_increasing_id().cast(IntegerType()))

    print_and_log("## Map ColumnTypes from Spark to Teradata")

    df = sdf.limit(1000).toPandas()

    #**************************************************************************************************************
    #**************************************************************************************************************
    ## Place Row_ID on the first column-position in your database.
    ## Teradata always use the first column as Primary Index if not explicitly specified in
    ## `INSERT INTO SELECT * FROM ` clause
    #**************************************************************************************************************
    #**************************************************************************************************************


    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    cols_specific = {}
    cols_specific["ROW_ID"] = ['INTEGER', 'INT']
    #cols_specific {COL_NAME: [TERATYPE, HIVE_TYPE]}

    isDateFmt = False
    PRIMARY_INDX = 'ROW_ID'

    str_ = \
    '''
    CREATE MULTISET TABLE {0},
        NO FALLBACK,
        NO BEFORE JOURNAL,
        NO AFTER JOURNAL,
        CHECKSUM = DEFAULT,
        DEFAULT MERGEBLOCKRATIO,
        MAP = TD_MAP2
        (
            ROW_ID INTEGER,
    '''
    for column_name, column in df.iteritems():
        try:
            if isinstance(column[column.first_valid_index()], str):
                if (df[column_name].str.len().max() >= 4000) or ('COMMONSEGMENTOUID' in column_name.upper()):
        #             df.drop(columns=[column_name], inplace=True)
                    str_+=column_name.upper() + ' ' +'CLOB, '
                    cols_specific[column_name] = ['CLOB', 'STRING']
                else:
                    if 'INN' in column_name.upper() or 'KPP' in column_name.upper():
                        str_+="\t" + column_name.upper() + ' ' +'VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,\n'
                        cols_specific[column_name] = ['VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC', 'STRING']
                    elif part_name.upper() in column_name.upper():
                        str_+="\t" + column_name.upper() + ' ' +"DATE FORMAT 'YYYY-MM-DD',\n"
                        cols_specific[column_name] = ["DATE FORMAT 'YYYY-MM-DD'", 'DATE_FORMAT({}, "yyyy-MM-dd")']
                    else:
                        str_+="\t" + column_name.upper() + ' ' +'VARCHAR(800) CHARACTER SET UNICODE NOT CASESPECIFIC,\n'
                        cols_specific[column_name] = ['VARCHAR(800) CHARACTER SET UNICODE NOT CASESPECIFIC', 'STRING']
            elif isinstance(column[column.first_valid_index()], np.integer) and (column_name.upper() != "ROW_ID"):
                if len(str(column[column.first_valid_index()])) < 1.e6:
                    str_+="\t" + column_name.upper() + ' ' +'INTEGER,\n'
                    cols_specific[column_name] =['INTEGER', 'INT']
                else:
                    str_+="\t" + column_name.upper() + ' ' +'INTEGER, '
                    cols_specific[column_name] =['INTEGER', 'BIGINT']
            elif (
                   ( isinstance(column[column.first_valid_index()], decimal.Decimal) ) or
                   ( isinstance(column[column.first_valid_index()], float) )
                 ):
        #         df[column_name] = df[column_name].fillna(0.0)
                str_+="\t" + column_name.upper() + ' ' +'FLOAT,\n'
                cols_specific[column_name] = ['FLOAT', 'FLOAT']
            elif (
                    isinstance(column[column.first_valid_index()], pd.Timestamp) or
                    isinstance(column[column.first_valid_index()], datetime.date)
                 ):
                if hasattr(column[column.first_valid_index()],'minute'):
                    if column[column.first_valid_index()].minute == 0:
                        isDateFmt = True
                    elif column[column.first_valid_index()].microsecond != 0:
                        isDateFmt = False
                        str_+="\t" + column_name.upper() + ' ' +"TIMESTAMP FORMAT 'YYYY-MM-DDBHH:MI:SS.S(6)',\n"
                        cols_specific[column_name] = ["TIMESTAMP FORMAT 'YYYY-MM-DDBHH:MI:SS.S(6)'",
                                                      'DATE_FORMAT({}, "yyyy-MM-dd HH:mm:ss.SSSSSS")']
                    else:
                        isDateFmt = False
                        str_+="\t" + column_name.upper() + ' ' +"TIMESTAMP FORMAT 'YYYY-MM-DDBHH:MI:SS',\n"
                        cols_specific[column_name] = ["TIMESTAMP FORMAT 'YYYY-MM-DDBHH:MI:SS'",
                                                      'DATE_FORMAT({}, "yyyy-MM-dd HH:mm:ss")']
                else:
                    isDateFmt = True
                if isDateFmt:
                    str_+="\t" + column_name.upper() + ' ' +"DATE FORMAT 'YYYY-MM-DD',\n"
                    cols_specific[column_name] = ["DATE FORMAT 'YYYY-MM-DD'", 'DATE_FORMAT({}, "yyyy-MM-dd")']
            else:
                None
        except:
            str_+=column_name.upper() + ' ' +'VARCHAR(800),\n'
            cols_specific[column_name] = ['VARCHAR(800)', 'STRING']

    res=str_.strip()[:-1] + '\n\t)' + '\n\tPRIMARY INDEX ({});'.format(PRIMARY_INDX)

    cr_tbl_sql = res
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    print_and_log("## Add Partitioning Instructions")

    if part_name is not None:
        PART_COL_NAME = part_name
        cr_part_tbl_query = cr_tbl_sql[:-1] + \
        "\n\tPARTITION BY RANGE_N({} BETWEEN DATE '2020-01-01' AND DATE '2025-01-01' EACH INTERVAL '1' DAY);".format(PART_COL_NAME)
        print_and_log(cr_part_tbl_query)
    else:
        cr_part_tbl_query = cr_tbl_sql
        print_and_log(cr_tbl_sql)


    print_and_log("## Batch Exporting to Local storage of Hive tbl using RDD MapPartitions with Index")

    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    COLS_ORDER_LST__ = ["ROW_ID"] + [col.upper() for col in sdf.columns if col not in ("row_id")]

    numPartitions = sdf.rdd.getNumPartitions()

    batches = np.array_split(np.arange(0, numPartitions), BATCH_SPLIT__)
    tuple_batches = [(i, batches[i]) for i in range(len(batches))]

    name_postfix , partitions = tuple_batches[0]

    typesmap_rdd={}

    for column_name, column in sdf.dtypes:
        if column == 'string':
            typesmap_rdd[column_name] = StringType()
        elif 'decimal' in column:
            digits = int('{}'.format(column.split('(')[1].split(',')[0]))
            prec   = int('{}'.format(column.split('(')[1].split(',')[1][:-1]))
            typesmap_rdd[column_name] = DecimalType(digits,prec)
        elif column == 'double':
            typesmap_rdd[column_name] = DoubleType()
        elif column == 'float':
            typesmap_rdd[column_name] = FloatType()
        elif column == 'int':
            typesmap_rdd[column_name] = IntegerType()
        elif column == 'bigint':
            typesmap_rdd[column_name] = LongType()
        elif column == 'timestamp':
            typesmap_rdd[column_name] = TimestampType()
        elif column == 'date':
            typesmap_rdd[column_name] = DateType()


    cols = [col for col,_ in sdf.dtypes]
    for i, partlst in tqdm(tuple_batches[:]):

        print("# Step {}; part length: {}".format(i, len(partlst)))
        res = sdf.rdd.mapPartitionsWithIndex(lambda i,it: collectRowsByIndex(i,it,indxs=partlst))

        currschema = StructType([StructField(col, typesmap_rdd[col]) for col in cols])
        sdf_proc = hive.createDataFrame(res, schema=currschema)

        sdf_proc.registerTempTable(tmp_tbl_name)

        _str = '''SELECT '''
        for col_name, spec in cols_specific.items():
            if '{' not in spec[1]:
                _str+='''CAST({col} AS {type}) {col}, '''.format(col=col_name, type=spec[1])
            else:
                _str+='''CAST({} as {}) {}, '''.format(spec[1].format(col_name),
                                                       spec[0].split("FORMAT")[0].split(" ")[0].strip(), col_name)
        fin_query = _str.strip()[:-1] + " FROM {}".format(tmp_tbl_name)

        sdf_casted = hive.sql(fin_query)
        sdf_casted = sdf_casted.select( ["row_id"]+[col for col in sdf.columns if col not in ("row_id")])

        df = sdf_casted.toPandas()
        df.to_csv(os.path.join(EXPORT_PATH, "{}_{}.csv".format(table_name, i)), sep=",", encoding="utf-8", index=False)
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    #/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    print_and_log("## GENERATE Config for FastLoad utility")
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    config_str=''

    config_str+=\
    '''
    logon {HOST}/{user}, {_pass};

    set record vartext  ",";
    record 2;

    database {db};
    drop table {db}.{tbl}_err;
    drop table {db}.{tbl}_err1;
    drop table {db}.{tbl}_err2;
    drop table {db}.{tbl};

    '''.format(HOST = TERADATA_HOST,
               user = USERNAME,
               _pass = PASSWORD,
               db   = DB,
               tbl  = table_name
              )

    config_str+=cr_part_tbl_query.format(table_name)

    config_str+=\
    '''
    create error table {db}.{tbl}_err for {db}.{tbl};
    begin loading {db}.{tbl}
        errorfiles {db}.{tbl}_err1, {db}.{tbl}_err2
        checkpoint 1500000;

    define

    '''.format(
               db   = DB,
               tbl  = table_name
              )

    in_str=\
    '''

    '''
    for col in COLS_ORDER_LST__:
        in_str+="in_{} (VARCHAR(500)),\n\t".format(col)
    in_str=in_str.strip()[:-1]

    config_str+=in_str

    config_str+=\
    '''

    file="U:\FAST_LOAD\csv\lal_db_hist_in_0.csv";

    SHOW;

    insert into {db}.{tbl} (

    '''.format(db=DB,
               tbl=table_name
              )

    in_str=\
    '''

    '''
    for col in COLS_ORDER_LST__:
        in_str+="{},\n\t".format(col)
    in_str=in_str.strip()[:-1]

    config_str+=in_str

    config_str+=\
    '''

    )

    values (

    '''

    in_str=\
    '''

    '''
    for col in COLS_ORDER_LST__:
        _type = cols_specific[col]
        if 'FORMAT' in _type[0]:
            fmt = _type[0].split("FORMAT")[-1].strip()
            in_str+=":in_{}(FORMAT {}),\n\t".format(col, fmt)
        else:
            in_str+=":in_{},\n".format(col)
    in_str=in_str.strip()[:-1]

    config_str+=in_str

    config_str+=\
    '''

    );

    end loading;
    logoff;
    '''
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    print_and_log("## Write CONFIG into file")

    with open(CONF_OUT_PATH__, "w", encoding="utf8") as fout:
    #     for ind, line in enumerate(config_str.split("\n")):
        fout.writelines(config_str)

    _str=\
    '''
    #######################################################################################################
    #######################################################################################################
    ## Copy the content of current `/opt/workspace/../TERA_FAST_LOAD` folder into VARM disk `U:\FAST_LOAD\`
    ## CHDIR -D U:\FAST_LOAD\
    ## run teradata loader executing command: `fastload -c utf8 <run.fld> log.txt`
    #######################################################################################################
    #######################################################################################################
    '''

    print_and_log(_str)

    print_and_log("BYE....")




