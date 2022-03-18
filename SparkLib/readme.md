## Examples of running spark context using different methods of instantinating *spark()* class

Working example can be found here: **./test_pyspark_wrapper.py**

```python
if __name__ == '__main__':

    print("### Instantiating Child of spark class oblect. Run")
    with NewSpark() as newcl:
        print(newcl.sp)

    print("### Starting static spark context. Kerberos AUTH = True!")

    with spark(schema=CONN_SCHEMA,
               dynamic_alloc=False,
               numofinstances=5,
               numofcores=8,
               executor_memory='25g',
               driver_memory='25g',
               kerberos_auth=True,
               process_label="TEST_PYSPARK_"
               ) as sp:

        hive = sp.sql
        print(sp.sc.version)

    print("### Starting static spark context. Kerberos AUTH = False!")

    with spark(schema=CONN_SCHEMA,
               dynamic_alloc=False,
               numofinstances=5,
               numofcores=8,
               executor_memory='25g',
               driver_memory='25g',
               kerberos_auth=False,
               process_label="TEST_PYSPARK_"
               ) as sp:

        hive = sp.sql
        print(sp.sc.version)


    print("### Starting static spark context from user-defined template. Run!")

    with spark(schema=CONN_SCHEMA,
               usetemplate_conf = True,
               default_template= PYSPARK_HIVE_STATIC_KRBR_TGT,
               process_label="TEST_PYSPARK_"
               ) as sp:

        hive = sp.sql
        print(sp.sc.version)

```

*spark()* class have build-in logging method so setting paramerer *process_label="TEST_PYSPARK_"* will generate log file with the same name as the string value assigned to *process_label*

User-Defined Templates (UDT) are located in folder *./pyspark_templates/*

After creating new template files one should add new user variables at *./tools.py* accordng to the names of newly created templates:
```python

__PYSPARK_TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'pyspark_templates')
PYSPARK_HIVE_STATIC_KRBR_TGT = os.path.join(__PYSPARK_TEMPLATE_DIR, 'pyspark_hive_static_krbr_tgt')

```

parameter *default_template= PYSPARK_HIVE_STATIC_KRBR_TGT* responds to the parsing appropriate template file and feeding ones key-val pairs to the *SparkConf().setall()* method

switching parameter *kerberos_auth=False|True* will enabling/disabling the following lines in spark config:
```python
    if self.kerberos_auth:
        self.conf = self.conf.set('spark.executor.extraJavaOptions','-Djavax.security.auth.useSubjectCredsOnly=false') \
                                .set('spark.driver.extraJavaOptions','-Djavax.security.auth.useSubjectCredsOnly=false') \
                                .set('spark.yarn.principal','{}@DF.SBRF.RU'.format(self.curruser)) \
                                .set('spark.yarn.keytab','/home/{}/keytab/user.keytab'.format(self.curruser))

```
If one prefers to use UDT files so it is necessary to create new template and setting to *True* the following params:
```bash
spark.executor.extraJavaOptions: -Djavax.security.auth.useSubjectCredsOnly=true
spark.driver.extraJavaOptions: -Djavax.security.auth.useSubjectCredsOnly=true

```


## Info
* If one will be using SparkConnector() class from labdada directory (./labdata/spark_connector.py) so don't forget to change following sparkConfig args:
1. .set('spark.hadoop.hive.metastore.uris', 'thrift://pklis-chdxxxxx.labiac.df.sbrf.ru:48869') where 'xxxxx' - is your digital letters for Hive Metasotre Server
1. Default path for teradata drivers is configured via TERADRIVER_PATH in spark() class
1. Set following line in ./labdata/sparkdb_loader.py based on your location of driver folder: TERADRIVER_PATH='/opt/workspace/{}/notebooks/drivers/'.format(curruser)

If you will encouter a error like "KERBEROS TGT No valid kerberos ticket... Preauthentication error" then regenerate your kerberos ticket using the following command:
```python
ipa-getkeytab -P -p login@DF.SBRF.RU -k ~/keytab/user.keytab
```

Then one can verify that created kerberos ticket is valid using the following line:

```bash
kinit -kt /home/$(whoami)/keytab/user.keytab $(whoami)
```

Don't forget to add following global variables into user .bash_profile depending on the preferred cloudera parcel:
```bash
export LABDATA_PYSPARK=/opt/workspace/$USER/notebooks/labdata_v1.2
export HADOOP_CONF_DIR=/etc/hive/conf
export SPARK_HOME=/opt/cloudera/parcels/SPARK2-2.2.0.cloudera4-1.cdh5.13.3.p0.603055/lib/spark2
export JAVA_HOME=/usr/java/jdk1.8.0_171-amd64
export PYSPARK_DRIVER_PYTHON=/opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bin/python
export PYSPARK_PYTHON=/opt/cloudera/parcels/PYENV.ZNO0059623792-3.5.pyenv.p0.2/bin/python
```

## REQUIREMENTS
- tendo
- cx_Oracle
