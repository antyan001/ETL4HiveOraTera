#!/opt/workspace/ektov1-av_ca-sbrf-ru/bin/python35

import os
import sys
import warnings
warnings.filterwarnings('ignore')

from collections import OrderedDict
from pathlib import Path
import pandas as pd
import numpy as np
import re
import joblib
import time
import hashlib
from datetime import datetime
import dateutil.relativedelta as relativedelta

pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.max_colwidth', -1)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# curruser = 'ektov1-av_ca-sbrf-ru'
curruser = os.environ.get('USER')

isUseOptWorkspace = False
# sys.path.insert(0, './../../src')

if isUseOptWorkspace:
    sys.path.insert(0, '/opt/workspace/{}/notebooks/ecom_model/src/'.format(curruser))
    sys.path.insert(0, '/opt/workspace/{}/notebooks/support_library/'.format(curruser))
    sys.path.insert(0, '/opt/workspace/{}/libs/python3.5/site-packages/'.format(curruser))
    sys.path.insert(0, '/opt/workspace/{}/notebooks/labdata/lib/'.format(curruser))
else:
    sys.path.insert(0, '/home/{}/notebooks/ecom_model/src/'.format(curruser))
    sys.path.insert(0, '/home/{}/notebooks/support_library/'.format(curruser))
    sys.path.insert(0, '/home/{}/python35-libs/lib/python3.5/site-packages/'.format(curruser))
    sys.path.insert(0, '/home/{}/notebooks/labdata/lib/'.format(curruser))


from urllib.parse import urlparse

# from tqdm import tqdm
# from tqdm._tqdm_notebook import tqdm_notebook
# tqdm_notebook.pandas()

from spark_connector import SparkConnector
from sparkdb_loader import spark
# from spark_helper import SparkHelper
from connector import OracleDB
import pyspark
from pyspark.storagelevel import StorageLevel
from pyspark import SparkContext, SparkConf, HiveContext
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.dataframe import DataFrame
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark import SparkFiles

def drop_col(df, cols: list):
    scol = sdf.columns
    final_cols = [i for i in scol if i not in cols]
    return df.select(*final_cols)

def show(self, n=10):
    return self.limit(n).toPandas()
pyspark.sql.dataframe.DataFrame.show = show


#==========================================================================================================
#================================== MAIN CLASS FOR COOKIE MATCHING ========================================
#==========================================================================================================

class CreateData():
    def __init__(self, sp,
                 isUseCTL: bool = False,
                 date_begin: str = '2021-07-01',
                 date_end: str   = '2021-08-01'):
        self.hive = sp.sql.sql
        self.sc = sp.sc

        self.schema          = 'sbx_team_digitcamp'
        self.schema_cib      = 'cib_custom_cib_ml360'
        self.schema_erib     = 'cap_part_erib__internal_erib_srb_ikfl'
        self.schema_sas      = 'cap_digital_feedback_internal_kakb_kakb_od'
        self.schema_od_visit = 'cap_external_google_analytics_up_external_google_analytics'

        self.date_begin = date_begin
        self.date_end = date_end
        self.isUseCTL = isUseCTL

        self.u_client_system = 'u_client_system'
        self.table_visit = 'visit'
        self.table_ctl_date_map = 'ga_visit_ctl_date_map'
        self.cookie_origin_tbl_name    = 'all_cookie_inn_match'

    def get_mapping(self) -> str:
        if self.isUseCTL:
            all_cookie_inn_match = hive.table("{}.{}".format(self.schema, self.cookie_origin_tbl_name))
            MAX_COOKIE_ACTIVE_DT = all_cookie_inn_match.selectExpr("max(COOKIE_ACTIVE_DT) as MAX_COOKIE_ACTIVE_DT").collect()[0]['MAX_COOKIE_ACTIVE_DT']
            REF_DT_STR = datetime.strftime(MAX_COOKIE_ACTIVE_DT, '%Y-%m-%d')

            selected_ctl_load=\
            hive.sql('''select ctl_loading from {t_ctl_data}
                        where min_sessiondate > timestamp('{ref}')'''.format(t_ctl_data = self.schema +"."+ self.table_ctl_date_map,
                                                                              ref=REF_DT_STR)
                    ).collect()
            ctl_load_str = ", ".join([str(ctl['ctl_loading']) for ctl in selected_ctl_load])

            return ctl_load_str
        else:
            sdf_ctl_date_map_month = hive.sql('''select
                                                      ctl_loading, min_sessiondate, max_sessiondate
                                                  from {t_ctl_data} --sbx_team_digitcamp.ga_visit_ctl_date_map
                                                  where min_sessiondate >= "{t_date_begin}" and max_sessiondate < "{t_date_end}"
                                               '''.format( t_ctl_data = self.schema +"."+ self.table_ctl_date_map,
                                                           t_date_begin = self.date_begin,
                                                           t_date_end   = self.date_end))

            ctl_date_map_month = list(map(str, sorted([row['ctl_loading'] for row in sdf_ctl_date_map_month.collect()])))
            str_ctl_date_month = ', '.join([ctl_date_map_month[0], ctl_date_map_month[-1]])

            return str_ctl_date_month


    def get_visit_sdf(self, str_ctl_date_month: str) -> pyspark.sql.dataframe.DataFrame:
        if self.isUseCTL:
            sdf_visit_part = hive.sql('''select       cid
                                                    , sboluserid
                                                    , sbboluserid
                                                    , hitPagePath
                                                    , timestamp(sessionDate)
                                                    , CASE WHEN
                                                        regexp_extract(hitPagePath,'.*dccvmid=([\\\\d\\\\\w\-\_]+).*',1) <> ''
                                                      THEN  regexp_extract(hitPagePath,'.*dccvmid=([\\\\d\\\\\w\-\_]+).*',1)
                                                      ELSE Null
                                                      END as inn_md5
                                                    , commonsegmentouid
                                                    , common_ym_uid
                                                    , ctl_loading
                                            from {t_visit}
                                            where (ctl_loading in ({t_ctl_arr}))  and
                                                  (hitPageHostName != 'localhost') and
                                                  (eventLabel not like '%ear%') and
                                                  (cid rlike "\\\\d+\.\\\\\d+")
                                            '''.format(t_visit = self.schema_od_visit +"."+ self.table_visit,
                                                       t_ctl_arr = str_ctl_date_month)
                                    )
        else:
            sdf_visit_part = hive.sql('''select       cid
                                                    , sboluserid
                                                    , sbboluserid
                                                    , hitPagePath
                                                    , timestamp(sessionDate)
                                                    , CASE WHEN
                                                        regexp_extract(hitPagePath,'.*dccvmid=([\\\\d\\\\\w\-\_]+).*',1) <> ''
                                                      THEN  regexp_extract(hitPagePath,'.*dccvmid=([\\\\d\\\\\w\-\_]+).*',1)
                                                      ELSE Null
                                                      END as inn_md5
                                                    , commonsegmentouid
                                                    , common_ym_uid
                                                    , ctl_loading
                                            from {t_visit}
                                            where (ctl_loading between {t_ctl_date_map_b} and {t_ctl_date_map_e}) and
                                                  (hitPageHostName != 'localhost') and
                                                  (eventLabel not like '%ear%') and
                                                  (cid rlike "\\\\d+\.\\\\\d+")
                                            '''.format(t_visit = self.schema_od_visit +"."+ self.table_visit,
                                                       t_ctl_date_map_b = str_ctl_date_month.split(',')[0],
                                                       t_ctl_date_map_e = str_ctl_date_month.split(',')[-1])
                                    )

        return sdf_visit_part


    def cookie_union(self, sdf_visit_part: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:

        sdf_visit_part.createOrReplaceTempView("sdf_visit_part")
    #     visit_inn_match.createOrReplaceTempView("visit_inn_match")

        query=\
        '''
        with cid_sbbol as(
            select
                distinct
                cid,
                sbboluserid
            from sdf_visit_part
            where (inn_md5 is not Null) or (sbboluserid is not Null)
        ),
        maxSessDate as(
            select
                 cid
                ,cast(max(sessionDate) as timestamp) as cookie_active_dt
            from sdf_visit_part
            where (cid is not Null) and ((inn_md5 is not Null) or (sbboluserid is not Null))
            group by cid
        ),
        cid_sbol as(
            select
                distinct
                cid,
                sboluserid
            from sdf_visit_part
            where (sboluserid is not Null and sboluserid <> 'stub')
        ),
        maxSessDateERIB as(
            select
                 cid
                ,cast(max(sessionDate) as timestamp) as cookie_active_dt
            from sdf_visit_part
            where (cid is not Null) and (sboluserid is not Null and sboluserid <> 'stub')
            group by cid
        ),
        cid_dccvmid as(
            select
                distinct
                cid,
                inn_md5
            from sdf_visit_part
            where inn_md5 is not Null
        ),
        cid_segmento as(
            select
                distinct
                cid,
                commonsegmentouid
            from sdf_visit_part
            where commonsegmentouid is not Null
        ),
        cid_ya as(
            select
                distinct
                cid,
                common_ym_uid
            from sdf_visit_part
            where common_ym_uid is not Null
        ),
        u_client as(
            select
                  distinct
                  inn
                , system_client_id
            from {t_mapping}
            where (inn is not Null) and (system_type_cd = "SBBOLUSERGUID")
        ),
        erib_users as(
            select
                  distinct
                  md5(cast(login_id as string)) sboluserid_erib
                , ucp_id as epk_id
            from {t_mapping_erib}
            where (login_id is not Null)
        ),
        final_match_erib(
        select
            distinct
              cid_sbol.cid
            , cid_sbol.sboluserid
            , erib_users.epk_id
            , cid_segmento.commonsegmentouid
            , cid_ya.common_ym_uid
            , maxdt.cookie_active_dt
            --, date_format(current_timestamp, 'yyyy-MM-dd') as load_dt
        from cid_sbol
        left join cid_segmento on cid_segmento.cid = cid_sbol.cid
        left join cid_ya on cid_ya.cid = cid_sbol.cid
        left join erib_users on erib_users.sboluserid_erib = cid_sbol.sboluserid
        left join maxSessDateERIB as maxdt on maxdt.cid = cid_sbol.cid
        where not (epk_id is Null and
                   commonsegmentouid is Null and
                   common_ym_uid is Null
                  )
        ),
        final_match_sbbol(
        select
            distinct
              cid_sbbol.cid
            , cid_sbbol.sbboluserid
            , u_client.inn
            , coalesce(md5(u_client.inn), cid_dccvmid.inn_md5) inn_md5
            , cid_segmento.commonsegmentouid
            , cid_ya.common_ym_uid
            , maxdt.cookie_active_dt
            --, date_format(current_timestamp, 'yyyy-MM-dd') as load_dt
        from cid_sbbol
        left join cid_segmento on cid_segmento.cid = cid_sbbol.cid
        left join cid_ya on cid_ya.cid = cid_sbbol.cid
        left join u_client as u_client on u_client.system_client_id = cid_sbbol.sbboluserid
        left join cid_dccvmid on cid_dccvmid.cid = cid_sbbol.cid
        left join maxSessDate as maxdt on maxdt.cid = cid_sbbol.cid
        where not (inn is Null and
                   inn_md5 is Null and
                   commonsegmentouid is Null and
                   common_ym_uid is Null
                   )
        ),
        DROP_NAN_DB AS(
        SELECT
            CID
           ,SBBOLUSERID
           ,INN
           ,INN_MD5
           ,COMMONSEGMENTOUID
           ,COMMON_YM_UID
           ,COOKIE_ACTIVE_DT
           ,CASE WHEN
            (FIRST(SBBOLUSERID) OVER (PARTITION BY CID, INN_MD5 ORDER BY INN_MD5) IS NULL) AND
            (FIRST(INN) OVER (PARTITION BY CID, INN_MD5 ORDER BY INN_MD5) IS NULL) AND
            (LEAD(SBBOLUSERID) OVER (PARTITION BY CID, INN_MD5 ORDER BY INN_MD5) IS NOT NULL) AND
            (LEAD(INN) OVER (PARTITION BY CID, INN_MD5 ORDER BY INN_MD5) IS NOT NULL)
            THEN 1
            ELSE 0
            END AS DROP_NAN
        FROM
            (
            select
            *
            from final_match_sbbol
            order by sbboluserid asc
            )
        ),
        FINAL_ERIB_COOKIE(
            SELECT
                 CID
               , CAST(NULL as STRING) SBBOLUSERID
               , SBOLUSERID
               , CAST(NULL as STRING) INN
               , CAST(NULL as STRING) INN_MD5
               , EPK_ID
               , COMMONSEGMENTOUID
               , COMMON_YM_UID
               , COOKIE_ACTIVE_DT
            FROM final_match_erib
        ),
        FINAL_SBBOL_COOKIE(
            SELECT
                 CID
               , SBBOLUSERID
               , CAST(NULL as STRING) SBOLUSERID
               , INN
               , INN_MD5
               , CAST(NULL as LONG) EPK_ID
               , COMMONSEGMENTOUID
               , COMMON_YM_UID
               , COOKIE_ACTIVE_DT
            FROM DROP_NAN_DB
            WHERE DROP_NAN <> 1
        )
        SELECT * FROM FINAL_SBBOL_COOKIE
        UNION ALL
        SELECT * FROM FINAL_ERIB_COOKIE
        '''.format(
                   t_mapping = self.schema_cib + "." + self.u_client_system,
                   t_mapping_erib = self.schema_erib + "." + "users"
                   )

        visit_enriched = hive.sql(query)

        return visit_enriched


    def cookie_union_erib(self, sdf_visit_part: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:

        sdf_visit_part.createOrReplaceTempView("sdf_visit_part")
    #     visit_inn_match.createOrReplaceTempView("visit_inn_match")

        query=\
        '''
        with cid_sbol as(
            select
                distinct
                cid,
                sboluserid
            from sdf_visit_part
            where (sboluserid is not Null and sboluserid <> 'stub')
        ),
        maxSessDate as(
            select
                 cid
                ,cast(max(sessionDate) as timestamp) as cookie_active_dt
            from sdf_visit_part
            group by cid
            having cid is not Null
        ),
        cid_segmento as(
            select
                distinct
                cid,
                commonsegmentouid
            from sdf_visit_part
            where commonsegmentouid is not Null
        ),
        cid_ya as(
            select
                distinct
                cid,
                common_ym_uid
            from sdf_visit_part
            where common_ym_uid is not Null
        ),
        erib_users as(
            select
                  distinct
                  md5(cast(login_id as string)) sboluserid_erib
                , ucp_id as epk_id
            from {t_mapping}
            where (login_id is not Null)
        ),
        final_match(
        select
            distinct
              cid_sbol.cid
            , cid_sbol.sboluserid
            , erib_users.epk_id
            , cid_segmento.commonsegmentouid
            , cid_ya.common_ym_uid
            , maxdt.cookie_active_dt
            --, date_format(current_timestamp, 'yyyy-MM-dd') as load_dt
        from cid_sbol
        left join cid_segmento on cid_segmento.cid = cid_sbol.cid
        left join cid_ya on cid_ya.cid = cid_sbol.cid
        left join erib_users on erib_users.sboluserid_erib = cid_sbol.sboluserid
        left join maxSessDate as maxdt on maxdt.cid = cid_sbol.cid
        where not (epk_id is Null and
                   commonsegmentouid is Null and
                   common_ym_uid is Null
                  )
        )
        SELECT
             CID
           , CAST(NULL as STRING) SBBOLUSERID
           , SBOLUSERID
           , CAST(NULL as STRING) INN
           , CAST(NULL as STRING) INN_MD5
           , EPK_ID
           , COMMONSEGMENTOUID
           , COMMON_YM_UID
           , COOKIE_ACTIVE_DT
        FROM final_match
        '''.format(t_mapping = self.schema_erib + "." + "users")

        visit_enriched = hive.sql(query)

        return visit_enriched

#==========================================================================================================
#==========================================================================================================
#
#
#==========================================================================================================
#=========================== ADDITIONAL FUNCTIONS FOR WRITE DATA INTO HDFS ================================
#==========================================================================================================

def createSDF(conn_schema, target_tbl, insert, part_cols_str, bucket_num, bucket_cols):

    hive.sql('''create table {schema}.{tbl} (
                                             {fields}
                                                )
                 PARTITIONED BY ({part_col_lst})
                 CLUSTERED BY ({bucket_cols}) INTO {bucket_num} BUCKETS STORED AS PARQUET
             '''.format(schema=conn_schema,
                        tbl=target_tbl,
                        fields=insert,
                        part_col_lst=part_cols_str,
                        bucket_num=bucket_num,
                        bucket_cols=bucket_cols)
            )

def insertToSDF(sdf, conn_schema, tmp_tbl, target_tbl, part_cols_str, bucket_cols):

    sdf.registerTempTable(tmp_tbl)

    hive.sql("""
    insert into table {schema}.{tbl}
    partition({part_col})
    select * from {tmp_tbl}
    cluster by ({bucket_cols})
    """.format(schema=conn_schema,
               tbl=target_tbl,
               tmp_tbl=tmp_tbl,
               part_col=part_cols_str,
               bucket_cols=bucket_cols)
            )

#==========================================================================================================
#==========================================================================================================

conn_schema = 'sbx_team_digitcamp'
origin_tbl_name = 'all_cookie_inn_match'
merged_tbl_name = 'all_cookie_inn_match_merge'
tmp_tbl_name    = 'all_cookie_inn_match_part'
#==========================================================================================================
#==========================================================================================================

if __name__ == '__main__':

sp = spark(schema='sbx_team_digitcamp',
           sparkVersion='2.2',
           dynamic_alloc=False,
           numofinstances=10,
           numofcores=8,
           kerberos_auth=True,
           replication_num=2,
           process_label="VISIT_SEARCH_")
print(sp.sc.version)
hive = sp.sql

hive.setConf("hive.exec.dynamic.partition","true")
hive.setConf("hive.exec.dynamic.partition.mode","nonstrict")
hive.setConf("hive.enforce.bucketing","false")
hive.setConf("hive.enforce.sorting","false")
hive.setConf("spark.sql.sources.partitionOverwiteMode","dynamic")
# hive.setConf("hive.exec.stagingdir", "/tmp/{}/".format(curruser))
# hive.setConf("hive.exec.scratchdir", "/tmp/{}/".format(curruser))
hive.setConf("hive.load.dynamic.partitions.thread", 1)


preprocessingData = CreateData(sp,
                               isUseCTL = True
                              )

str_ctl_date_ = preprocessingData.get_mapping()
sdf_visit_resp = preprocessingData.get_visit_sdf(str_ctl_date_)

union_cookies = preprocessingData.cookie_union(sdf_visit_resp)
# sdf_erib = preprocessingData.cookie_union_erib(sdf_visit_resp)

#===================================================================================================
# UNION ALL COOKIES
#===================================================================================================

# union_cookies = sdf_sbbol.union(sdf_erib)
# union_cookies.registerTempTable("tmp_union_cookies")

#===================================================================================================
# SAVE UNIONED COOKIES into TMP TABLE
#===================================================================================================

hive.sql(''' drop table if exists {}.{} purge '''.format(conn_schema, tmp_tbl_name))
union_cookies.registerTempTable('tmp_fin_matching')
hive.sql('''create table {}.{} as select * from tmp_fin_matching'''.format(conn_schema, tmp_tbl_name))

#===================================================================================================
##MERGE TBLS SUBPROCESS
#===================================================================================================
all_cookie_inn_match = hive.table("{}.{}".format(conn_schema, origin_tbl_name))
all_cookie_inn_match.registerTempTable("tmp_all_cookie_inn_match")

all_cookie_inn_match_part = hive.table("{}.{}".format(conn_schema, tmp_tbl_name))
all_cookie_inn_match_part.registerTempTable("tmp_all_cookie_inn_match_part")

#===================================================================================================
## RUN MERGING: SQL Query
#===================================================================================================
mergequery=\
'''
with cookie_target as(
select
     CID
    ,SBBOLUSERID
    ,SBOLUSERID
    ,INN
    ,INN_MD5
    ,EPK_ID
    ,COMMONSEGMENTOUID
    ,COMMON_YM_UID
    ,COOKIE_ACTIVE_DT
from tmp_all_cookie_inn_match
),
cookie_source as(
select
     CID
    ,SBBOLUSERID
    ,SBOLUSERID
    ,INN
    ,INN_MD5
    ,EPK_ID
    ,COMMONSEGMENTOUID
    ,COMMON_YM_UID
    ,COOKIE_ACTIVE_DT as S_ACTIVE_DT
from tmp_all_cookie_inn_match_part
),
cookie_merged as (
select
      coalesce(t.CID, s.CID) CID
    , coalesce(t.SBBOLUSERID,s.SBBOLUSERID) SBBOLUSERID
    , coalesce(t.SBOLUSERID,s.SBOLUSERID) SBOLUSERID
    , coalesce(t.INN,s.INN) INN
    , coalesce(t.INN_MD5,s.INN_MD5) INN_MD5
    , coalesce(t.EPK_ID,s.EPK_ID) EPK_ID
    , coalesce(t.COMMONSEGMENTOUID,s.COMMONSEGMENTOUID) COMMONSEGMENTOUID
    , coalesce(t.COMMON_YM_UID,s.COMMON_YM_UID) COMMON_YM_UID
    , CASE WHEN (t.COOKIE_ACTIVE_DT < s.S_ACTIVE_DT) OR
                (t.COOKIE_ACTIVE_DT IS NULL)
           THEN s.S_ACTIVE_DT
           ELSE t.COOKIE_ACTIVE_DT
      END AS COOKIE_ACTIVE_DT
    --, s.S_ACTIVE_DT
from cookie_target as t
full outer join cookie_source as s
on t.CID               = s.CID and
   t.SBBOLUSERID       = s.SBBOLUSERID and
   t.INN               = s.INN and
   t.INN_MD5           = s.INN_MD5 and
   (
    (t.COMMONSEGMENTOUID IS NOT DISTINCT FROM s.COMMONSEGMENTOUID) and
    (t.COMMON_YM_UID     IS NOT DISTINCT FROM s.COMMON_YM_UID)  and
    (t.SBOLUSERID        IS NOT DISTINCT FROM s.SBOLUSERID) and
    (t.EPK_ID            IS NOT DISTINCT FROM s.EPK_ID)
   )
)
SELECT * FROM cookie_merged
'''

merged_sdf = hive.sql(mergequery)
merged_sdf.registerTempTable("all_cookie_merged")

#===================================================================================================
## SAVE MERGED TABLE
#===================================================================================================

hive.sql("DROP TABLE IF EXISTS  {sh}.{tbl} PURGE".format(sh=conn_schema, tbl=merged_tbl_name))

msql=\
'''
CREATE TABLE {sh}.{tbl}
SELECT * FROM all_cookie_merged
'''.format(sh=conn_schema, tbl=merged_tbl_name)

hive.sql(msql)

#===================================================================================================
## DROP/RENAME ORIGIN TABLE
#===================================================================================================

hive.sql("DROP TABLE IF EXISTS  {sh}.{tbl} PURGE".format(sh=conn_schema, tbl=origin_tbl_name))
hive.sql("DROP TABLE IF EXISTS  {sh}.{tbl} PURGE".format(sh=conn_schema, tbl=tmp_tbl_name))

hive.sql("ALTER TABLE {sh}.{merged_tbl} RENAME TO {orig_tbl}".format(sh=conn_schema,
                                                                     merged_tbl=merged_tbl_name,
                                                                     orig_tbl=origin_tbl_name))
