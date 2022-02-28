import os, sys, subprocess, importlib

_labdata = os.environ.get("LABDATA_PYSPARK")
sys.path.insert(0, _labdata)
os.chdir(_labdata)

from lib.tools import *
from lib.config import *

class SparkConnector(object):

    def __init__(self,
                 title='Test_app',
                 dynamic_allocation=False,
                 spark_version=None,
                 queue = 'g_dl_u_corp',
                 usetemplate_conf = False,
                 default_template= PYSPARK_HIVE_STATIC_KRBR_TGT,
                 numofinstances=20,
                 numofcores=8,
                 executor_memory='45g',
                 driver_memory='45g',
                 localdir = None,
                 sets_tuple=None,
                 replication_num=2,
                 kerberos_auth=False,
                 TERADRIVER_PATH=None,
                 need_oracle=True,
                 need_teradata=True):
        if dynamic_allocation:
            print('dynamic_allocation is ON')
        self.title = title
        self.dynamic_allocation = dynamic_allocation
        self.conf = None
        self.sc = None
        self.logger = None
        self.spark_version = spark_version
        self.sets_tuple = sets_tuple
        self.queue = queue
        self.numofinstances = numofinstances
        self.numofcores = numofcores
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.curruser = os.environ.get('USER')
        if localdir is None:
          self.localdir = '/home/{}/tmp/'.format(self.curruser)
        self.replication_num = replication_num
        self.kerberos_auth = kerberos_auth
        self.DRIVER_PATH = TERADRIVER_PATH
        self.need_oracle   = need_oracle
        self.need_teradata = need_teradata

        self.default_template = default_template
        self.usetemplate_conf = usetemplate_conf

        self.env_init()

    def get_spark_context(self):
        if self.conf:
            return self.sc
        else:
            self.env_init()
            return self.spark_init()

    def stop (self):
        self.sc.stop()

    def env_init(self):
        if self.spark_version == '2.2':
          spark_home = os.environ.get('SPARK_HOME')
        else:
          spark_home = '/opt/cloudera/parcels/SPARK2/lib/spark2/'
        # os.environ['SPARK_HOME'] = spark_home
        # os.environ['HADOOP_CONF_DIR'] = '/etc/hive/conf'
        # os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
        # os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bin/python'
        sys.path.insert(0, os.path.join(spark_home, 'python'))
        sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.7-src.zip'))
        pyspark = importlib.import_module('pyspark')
        global SparkContext, SparkConf, HiveContext
        from pyspark import SparkContext, SparkConf, HiveContext

    def spark_init(self):
        if not self.usetemplate_conf:
            if self.dynamic_allocation:
                self.conf = SparkConf().setAppName(self.title) \
                    .setMaster("yarn") \
                    .set('spark.yarn.driver.memoryOverhead', '4048mb')  \
                    .set('spark.hadoop.dfs.replication', str(self.replication_num)) \
                    .set('spark.yarn.executor.memoryOverhead', '4g')  \
                    .set('spark.port.maxRetries', '150') \
                    .set('spark.dynamicAllocation.enabled', 'true') \
                    .set('spark.ui.port', '4444') \
                    .set('spark.ui.showConsoleProgress', 'true') \
                    .set('spark.driver.extraClassPath', '/opt/cloudera/parcels/PYENV.ZNO0059623792/usr/lib/oracle/12.2/client64/lib/ojdbc8.jar') \
                    .set('spark.executor.extraClassPath', '/opt/cloudera/parcels/PYENV.ZNO0059623792/usr/lib/oracle/12.2/client64/lib/ojdbc8.jar')
            else:
                #hdfs://clsklod:8020/
                self.conf = SparkConf().setAppName(self.title) \
                    .setMaster("yarn") \
                    .set('spark.hadoop.hive.metastore.uris', 'thrift://pklis-chd001999.labiac.df.sbrf.ru:48869')\
                    .set('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation','true')\
                    .set('spark.yarn.access.hadoopFileSystems', 'hdfs://hdfsgw:8020/,hdfs://nsld3:8020/,hdfs://clsklcib:8020/,hdfs://clsklsbx:8020/,hdfs://clsklsmd:8020/')\
                    .set('spark.local.dir',self.localdir) \
                    .set('spark.hadoop.dfs.replication', str(self.replication_num)) \
                    .set('spark.executor.memory',self.executor_memory) \
                    .set('spark.driver.memory',self.driver_memory) \
                    .set('spark.shuffle.spill.numElementsForceSpillThreshold','50000') \
                    .set('spark.sql.shuffle.partitions', '400')\
                    .set('spark.memoryFraction','0.4') \
                    .set('spark.sql.autoBroadcastJoinThreshold',20*1024*1024)\
                    .set('spark.driver.maxResultSize','90g') \
                    .set('spark.driver.allowMultipleContexts', 'true')\
                    .set('spark.driver.memoryOverhead', '4048mb')  \
                    .set('spark.executor.memoryOverhead', '8g')  \
                    .set('spark.port.maxRetries', '550') \
                    .set('spark.executor.cores', str(self.numofcores)) \
                    .set('spark.executor.instances', str(self.numofinstances)) \
                    .set('spark.dynamicAllocation.enabled', 'false') \
                    .set('spark.default.parallelism','80') \
                    .set('spark.ui.port', '4445') \
                    .set('spark.ui.showConsoleProgress', 'true') \
                    .set('spark.kryoserializer.buffer.max','1024mb') \
                    .set('spark.blacklist.enabled', 'true')\
                    .set('spark.blacklist.task.maxTaskAttemptsPerNode', '13')\
                    .set('spark.blacklist.task.maxTaskAttemptsPerExecutor', '13')\
                    .set('spark.task.maxFailures', '15')\
                    .set('spark.driver.extraLibraryPath', '/opt/workspace/{}/notebooks/support_library/'.format(self.curruser))\
                    .set('spark.executor.extraLibraryPath', '/opt/workspace/{}/notebooks/support_library/'.format(self.curruser))\
                    .set("spark.sql.parquet.binaryAsString", "true")\
                    .set("spark.sql.hive.convertMetastoreParquet", "false")\
                    .set("spark.sql.parquet.mergeSchema", "false")\
                    .set("spark.sql.parquet.filterPushdown", "false")\
                    .set("spark.sql.files.ignoreCorruptFiles", "true")\
                    .set("spark.sql.hive.metastorePartitionPruning", "true")\
                    .set("spark.sql.sources.partitionOverwiteMode","dynamic")\
                    .set("spark.hadoop.parquet.enable.summary-metadata", "false")\
                    .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")\
                    .set('spark.hadoop.hive.exec.max.dynamic.partitions','50000')\
                    .set('spark.hadoop.hive.exec.max.dynamic.partitions.pernode','10000')
                if self.need_oracle:
                    self.conf = self.conf \
                        .set('spark.driver.extraClassPath',
                             '/opt/cloudera/parcels/PYENV.ZNO0059623792/usr/lib/oracle/12.2/client64/lib/ojdbc8.jar') \
                        .set('spark.executor.extraClassPath',
                             '/opt/cloudera/parcels/PYENV.ZNO0059623792/usr/lib/oracle/12.2/client64/lib/ojdbc8.jar')

                if self.need_teradata:
                    self.conf = self.conf.set('spark.jars', '{p}/terajdbc4.jar,{p}/tdgssconfig.jar'.format(p=self.DRIVER_PATH))

                if self.kerberos_auth:
                	self.conf = self.conf.set('spark.executor.extraJavaOptions','-Djavax.security.auth.useSubjectCredsOnly=false') \
    						                .set('spark.driver.extraJavaOptions','-Djavax.security.auth.useSubjectCredsOnly=false') \
    						                .set('spark.yarn.principal','{}@DF.SBRF.RU'.format(self.curruser)) \
    						                .set('spark.yarn.keytab','/home/{}/keytab/user.keytab'.format(self.curruser))

        else:
            pyconfig = parse_config(self.default_template)
            self.conf = SparkConf().setAppName(self.title) \
                            .setMaster("yarn").setAll(list(pyconfig.items()))



        # Custom sets
        if self.sets_tuple is not None:
            for set in self.sets_tuple:
                self.conf = self.conf.set(set[0], set[1])

        self.sc = SparkContext(conf=self.conf)
        self.sc.setLogLevel('ERROR')
        log4j_logger = self.sc._jvm.org.apache.log4j
        self.logger = log4j_logger.LogManager.getLogger('jobLogger')
        self.logger.setLevel(log4j_logger.Level.ERROR)
        return self.sc

    def get_sql_context(self):
        return HiveContext(self.sc)
