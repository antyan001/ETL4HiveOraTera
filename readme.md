# ETL scripts for CRUD communication between Hive-Oracle-Teradata using PySpark with jDBC as backend driver  

*PROJECT STRUCTURE*:
- `cxOracle_Loader/`
    - `loader.py`: class with Oracle DB connection and build-in getter/loader methods for working with data rows 
  in parallel/nonparallel batch modes
- `SparkLib`: main pySpark wrapper for submitting/running jobs on YARN Cluster
    - `spark_connector.py`: main class with spark hadoop configuration and Hive support
    - `sparkdb_loader.py`: wrapper over `spark_connector` with additional build-in methods for data manipulation
    - `connector.py`: connector to Oracle server
## `HIVE2TERA`: Added new HowTo examples highliting different ways of pythonic communication between Teradata DWH and Hadoop File System using jDBC driver
### Repos with JDBC-ETL scripts compartible with ojdbc8.jar and terajdbc4.jar drivers:
- `ETL_HiveOracle/`: 
    - `jupyter_examples/`: various .ipynb examples for working with Oracle DB using jDBC driver at the backend
    - `runsh/`: includes various `.py` scrips with implemented methods for loading data from Oracle/Teradata storage and 
    their subsequent insertion into Hive
        - `/runsh/ma_cmdm_ma_deal_new.py`: example of switching between Tera and Oracle jDBC drivers to insert new data into Hive
    - `/src/`:
        - `etl.py`: ETLORA class for reading/writing data from Oracle2Hive HDFS space 
- `ETL_HiveTeradata/`:
    - `jdbsHive2TeraDriver.ipynb`: .ipynb example covering different techniques for communicating with Teradata from HiveContext: `jaydebeapi`, `jDBC` 
    - `*.py`: various .py examples for working with Teradata DB using `jaydebeapi` lib and `jDBC` driver at the backend 
    - `/src/`:
        - `etl.py`: ETLTERA class for reading/writing data from Teradata2Hive HDFS space 
## Miscellaneous spark scripts:
- `PySparkScripts`: different scripts/notebooks covering various DE techniques for working with high-load HDFS data transformations  
- `YarnJobsAutoKiller`: shell `autokiller` of spark jobs running on YARN cluster       

- `OraTeraSwitcherETL`:
- `ma_cmdm_ma_deal_new.py`: example of switching between Tera and Orcale jDBC drivers to insert new data into Hive
- `jdbsHive2TeraDriver.ipynb`: various examples of Teradata connenction via `jDBC` driver (upload rows from Hive to Teradata and back again)


