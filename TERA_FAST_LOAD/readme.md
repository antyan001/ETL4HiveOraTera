## `FASTLOAD`: Added script for parallel transfort loading into Teradata from user-defined csv files
- /TERA_FAST_LOAD:
  - /csv: location of user files to be loaded by FL utility
  - `genFLConfig.py`: main script for autogeneration of config file `run.fld` containing all instructions for Tera bulk loader

### HOWTO use *FastLoad* utiity:
- Run `genFLConfig.py`
- Change the following params depending on your configuration:
  `tmp_tbl_name` = "tmp_hive2tera_cast" : *temp table name for local exporting via pySpark (can be left as is)*
  `EXPORT_PATH` = "/opt/workspace/{curruser}/notebooks/TERA_FAST_LOAD/csv": *path to local storage*
  `CONF_OUT_PATH__` = "/opt/workspace/{curruser}/notebooks/TERA_FAST_LOAD/run.fld" : *path to export FL config*
  `numdays` = 15: set the fix number of sorted patitions to catch from metastore (in the case of partitioned tbl)
  `BATCH_SPLIT__` = 2: set number of batches to split the origin Hive table into ones
- Copy the content of current `/opt/workspace/../TERA_FAST_LOAD` folder into VARM disk `U:\FAST_LOAD\`
- CHDIR -D U:\FAST_LOAD\
- Run `Teradata` loader executing the command: `fastload -c utf8 <run.fld> log.txt`
