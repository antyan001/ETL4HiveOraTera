import os
import sys
import subprocess
dir = os.path.dirname(os.path.realpath(__file__))
curruser = os.environ.get('USER')
# Конфигурация для ЛД 2.0
spark_home = '/opt/cloudera/parcels/SPARK2/lib/spark2/'
# spark_home = '/home/korsakov.n.n/spark_220'
os.environ['SPARK_HOME'] = spark_home
os.environ['PYSPARK_DRIVER_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO0059623792/bin/python'
os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO0059623792/bin/python'
#os.environ['LD_LIBRARY_PATH'] = '/opt/cloudera/parcels/PYENV.ZNO20008661/usr/lib/oracle/12.2/client64/lib'
os.environ['LD_LIBRARY_PATH'] = '/opt/cloudera/parcels/PYENV.ZNO0059623792/usr/lib/oracle/12.2/client64/lib'

sys.path.insert(0, os.path.join (spark_home,'python'))
sys.path.insert(0, os.path.join (spark_home,'python/lib/py4j-0.10.7-src.zip'))

from spark_connector import SparkConnector
from formatter import colprint

from connector import OracleDB
from pyspark import SparkContext, SparkConf, HiveContext
from lib.tools import *

os.environ['NLS_LANG']='RUSSIAN_CIS.AL32UTF8'

class spark:

    def outputLabel (self):
        return '{h}'+self.output+'{g}'

    def __init__(self,
                 schema='t_ural_kb',
                 sparkVersion='2.2',
                 output=None,
                 process_label='anthony_',
                 dynamic_alloc=False,
                 queue = 'g_dl_u_corp',
                 numofinstances=20,
                 numofcores=8,
                 executor_memory='45g',
                 driver_memory='45g',
                 usetemplate_conf=False,
                 default_template= PYSPARK_HIVE_STATIC_KRBR_TGT,
                 localdir = None,
                 sets_tuple=None,
                 replication_num=2,
                 kerberos_auth=True,
                 need_oracle=True,
                 need_teradata=True,
                 TERADRIVER_PATH='/opt/workspace/{}/notebooks/drivers/'.format(curruser)):
        self.db = schema
        self.conn = SparkConnector(title=process_label+'_'+schema,
                                   dynamic_allocation=dynamic_alloc,
                                   spark_version = sparkVersion,
                                   queue = queue,
                                   numofinstances = numofinstances,
                                   numofcores = numofcores,
                                   executor_memory = executor_memory,
                                   driver_memory = driver_memory,
                                   usetemplate_conf=usetemplate_conf,
                                   default_template=default_template,
                                   localdir = localdir,
                                   sets_tuple=sets_tuple,
                                   replication_num=replication_num,
                                   kerberos_auth=kerberos_auth,
                                   TERADRIVER_PATH=TERADRIVER_PATH,
                                   need_oracle = need_oracle,
                                   need_teradata = need_teradata)
        self.sc = self.conn.get_spark_context()
        self.sql = self.conn.get_sql_context()
        self.output = output


        self.currdate = datetime.strftime(datetime.today(), format='%Y-%m-%d')
        self.script_name = process_label
        self.sing = tendo.singleton.SingleInstance()

        self.init_logger()

        log("# __init__ : begin", self.logger)


    def init_logger(self):
        self.print_log = True

        try:
            os.makedirs("./logs/" + self.currdate)
        except:
            pass

        logging.basicConfig(filename='./logs/{}/{}.log'.format(self.currdate, self.script_name),
                            level=logging.INFO,
                            format='%(asctime)s %(message)s')
        self.logger = logging.getLogger("SparkClass")

        log("="*54 + " {} ".format(self.currdate) + "="*54, self.logger)

    def __enter__(self):
        return self

    def close(self):
        try:
            self.sc.stop()
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



    def save(self, df):
        df.registerTempTable(self.output)
        self.sql.sql('DROP TABLE IF EXISTS {db}.{tab}'.format(db=self.db, tab=self.output))
        self.sql.sql('CREATE TABLE {db}.{tab} SELECT * FROM {tab}'.format(db=self.db, tab=self.output))
        colprint('Datarame has been saved at {db}.{tab}'.format(db=self.db, tab=self.output))
        return self

    def get (self, input, where=None):
        if where!=None:
            self.sql.sql('SELECT * FROM {db}.{tab} WHERE {where}'.format(db=self.db, tab=input, where=where))
        return self.sql.sql('SELECT * FROM {db}.{tab}'.format(db=self.db, tab=input))

    def get_specific_cols (self, input, cols):
        return self.sql.sql('SELECT {cols} FROM {db}.{tab}'.format(cols=', '.join(cols), db=self.db, tab=input))

    def get_columns(self, input):
        return self.sql.sql('SELECT * FROM {db}.{tab} limit 1'.format(db=self.db, tab=input)).columns

    def exit (self):
        self.sc.stop()
        colprint('type n to save the data, any other key will clear the screen', True)
        if (input()!='n'):
            subprocess.call(['clear'])

    def get_csv(self, file_name, delimiter=",", directory = dir+'/../data/'):
        local_path = directory+file_name
        local_path += '' if local_path[-3:] != '.csv' else '.csv'
        print(local_path)

        subprocess.call(['hdfs', 'dfs', '-rm', '-r', file_name])
        colprint('file is being moved to HDFS')
        subprocess.call(['hdfs', 'dfs', '-copyFromLocal', local_path, file_name])
        colprint(file_name+' was copied from local folder to HDFS')

        self.save( self.sql.read.format('csv').option('header', 'true').option("delimiter", delimiter).load(file_name) )
        proc = subprocess.call(['hdfs', 'dfs', '-rm', '-r', file_name])

        return self

    def save_to_csv(self, df, sep:str, username:str,  hdfs_path:str, local_path:str=None, isHeader='true'):
        """
        Сохраняет Spark DataFrame с csv и создает линк на этот файл в файловой системе Jupyter

        Parameters
        ----------
        username:
            Имя пользователя в ЛД
        hdfs_path:
            Путь для сохранения файла в HDFS относительно папки пользователя (например notebooks/data)
        local_path:
            Путь, по которому будет доступен файл в файловой системе Jupyter (/home)
            Если None - запись производится только в hdfs
        """
        import subprocess
        import os

        path_to_hdfs = os.path.join('/user', username, hdfs_path)

        df.write \
            .format('com.databricks.spark.csv') \
            .mode('overwrite') \
            .option('sep', sep) \
            .option('header', isHeader) \
            .option("quote", '\u0000') \
            .save(path_to_hdfs)

        if local_path!=None:
            path_to_local = os.path.join('/home', username, local_path)
            proc = subprocess.call(['hdfs', 'dfs', '-getmerge', path_to_hdfs, path_to_local])
            proc = subprocess.call(['hdfs', 'dfs', '-rm', '-r', path_to_hdfs])
            #proc.communicate()
            columns_row = sep.join(df.columns)
            os.system("sed -i -e 1i'" + columns_row + "\\' " + path_to_local)

        return self

    def insert_to_hive(self, sdf, out_table,
                      mode = 'overwrite', frmt = None,
                      partitionCols = None, bucketCols = None, bucketNum = None):
        """Writes Spark Dataframe to the database, performs partitioning and bucketing.

        Parameters
        ----------
        TODO

        """

        # bucketing
        if bucketCols is not None:

            if bucketNum is not None:

                sdf = sdf.repartition(bucketNum, *bucketCols)

            else:

                sdf = sdf.repartition(*bucketCols)

        elif bucketNum is not None:

                sdf = sdf.repartition(bucketNum)


        writer = sdf.write.mode(mode)


        if frmt is not None:

            writer = writer.format(frmt)

        # partitioning
        if partitionCols is not None:
            writer = writer.partitionBy(*partitionCols)

        # write to hive
        if mode == 'overwrite':
            mode_header = 'Overwriting'
        elif mode == 'append':
            mode_header = 'Appending to'
        else:
            mode_header = '<UNKNOWN MODE>'

        log('{0} table {1}'.format(mode_header, out_table), self.logger)

        writer.insertInto('{}'.format(out_table))

        log('Writing complete', self.logger)



    def get_minmax(self, field, table):
        colprint('calculating min and max dates over the table {table}...'.format(table=table))
        return self.sql.sql('SELECT min({field}) as min, max({field}) as max FROM {db}.{table}'\
            .format(field=field, table=table, db=self.db)).toPandas().iloc[0]

    def get_teradata(self, db, table, BATCH_SIZE=1000, part=None):
        if part is None:
          return self.sql \
              .read.format('jdbc') \
              .option('url', db.dsn) \
              .option('user', db.user) \
              .option('password', db.password) \
              .option('dbtable', table) \
              .option('driver', 'com.teradata.jdbc.TeraDriver') \
              .option('fetchsize', BATCH_SIZE) \
              .load()
        else:
          return self.sql \
              .read.format('jdbc') \
              .option('url', db.dsn) \
              .option('user', db.user) \
              .option('password', db.password) \
              .option('dbtable', table) \
              .option('driver', 'com.teradata.jdbc.TeraDriver') \
              .option('numPartitions',part) \
              .option('fetchsize', BATCH_SIZE) \
              .load()

    def get_oracle(self, db, table, BATCH_SIZE=1000, part=None):
        if part is None:
          return self.sql \
              .read.format('jdbc') \
              .option('url', 'jdbc:oracle:thin:@//'+db.dsn) \
              .option('user', db.user) \
              .option('password', db.password) \
              .option('dbtable', table) \
              .option('driver', 'oracle.jdbc.driver.OracleDriver') \
              .option('fetchsize', BATCH_SIZE) \
              .load()
        else:
          return self.sql \
              .read.format('jdbc') \
              .option('url', 'jdbc:oracle:thin:@//'+db.dsn) \
              .option('user', db.user) \
              .option('password', db.password) \
              .option('dbtable', table) \
              .option('driver', 'oracle.jdbc.driver.OracleDriver') \
              .option('numPartitions',part) \
              .option('fetchsize', BATCH_SIZE) \
              .load()

    def hive_to_oracle(self, odb, target_table, hive_table = None, mode = 'overwrite', cols=None):

        db = OracleDB(odb)
        table = self.output if hive_table == None else hive_table
        print(cols)
        #a = 'DESCRIPTION clob, KEYWORDS clob, MAINCONTENT clob, EMAIL clob, NAME clob, PHONES clob, PHONESLABELS clob'

        return self.sql \
            .sql( 'SELECT * FROM {db}.{tab}'.format(db=self.db, tab=table) ) \
            .write \
            .format('jdbc') \
            .mode(mode) \
            .option('url', 'jdbc:oracle:thin:@//'+db.dsn) \
            .option('user', db.user) \
            .option('password', db.password) \
            .option('dbtable', target_table) \
            .option('createTableColumnTypes', cols)\
            .option('driver', 'oracle.jdbc.driver.OracleDriver') \
            .save()

    def csv_to_oracle(self, odb, target_table, file_name, delimiter=",", directory = dir+'/../data/', mode = 'overwrite'):
        self.output = self.output if target_table == None else target_table
        self.get_csv(file_name, delimiter, directory)

        return self.hive_to_oracle(odb, target_table, self.output, mode)

    def save_csv(self, df, file_name, delimiter=',', directory = dir+'/../data/'):
        local_path = directory+'%s.csv' % file_name
        hdfs_path = file_name

        proc = subprocess.call(['hdfs', 'dfs', '-rm', '-r', hdfs_path])
        df.coalesce(1).write.option('delimiter', delimiter).csv(hdfs_path, header=True)
        proc = subprocess.call(['hdfs', 'dfs', '-getmerge', hdfs_path, local_path])
        proc = subprocess.call(['hdfs', 'dfs', '-rm', '-r', hdfs_path])

        colprint('The file %s has been saved in {h}data{g} folder' % file_name)

        return self

    def open_csv_from_hdfs(self, hdfs_file_path, encoding='utf8', delimiter=','):
        #sqlContext = self.get_sql_context()
        df = self.sql.read.format("com.databricks.spark.csv") \
            .option('header', 'true') \
            .option('delimiter', delimiter) \
            .option('decimal', '.') \
            .option('encoding', encoding) \
            .option('dateFormat', 'YYYY-MM-dd') \
            .load(hdfs_file_path)
        return df

    def vec_to_array(self, col):
        #transforms vector column to array
        from pyspark.sql.functions import udf
        from pyspark.sql.types import ArrayType, DoubleType
        def to_array_(v):
            return v.toArray().tolist()
        return udf(to_array_, ArrayType(DoubleType()))(col)

