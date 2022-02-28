# -*- coding: utf-8 -*-
import os
import sys
import pandas as pd
import time
import argparse
import pathlib
import subprocess


def archive(folder):
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), folder)

    only_files = [f for f in os.listdir(path) if (f != 'archive')]
    pathlib.Path(os.path.join(path, "archive")).mkdir(parents=True, exist_ok=True)

    for file in only_files:
        old_file_path = os.path.join(path, file)
        new_file_path = os.path.join(path, os.path.join("archive", file))
        os.rename(old_file_path, new_file_path)


def get_hdfs_hive_table_path(sqlc, table, folder):
    """
    Получить hdfs путь, где хранится указанная таблица на hdfs
    """
    import pyspark.sql.functions as F
    df = sqlc.sql("describe formatted {}".format(table)).toPandas()
    # ищем для yarn кластера
    df_yarn = df[df['col_name'] == 'Location:']['data_type']
    if df_yarn.shape[0] != 0:
        hdfs_path = str(df_yarn.iloc[0])
    else:
        # ищем по регулярке для локального кластера
        df = sqlc.sql("describe extended {}".format(table)).filter(F.col("col_name") == "Location").collect()
        if len(df) > 0:
            hdfs_path = df[0][1]
        else:
            raise Exception("Did not found path in hdfs for table {}!".format(table))
    return hdfs_path


def table_size(hdfs_path):
    p = subprocess.Popen(['hdfs', 'dfs', '-du', '-s', hdfs_path], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    return p.communicate()[0].decode('utf-8').split(" ")[0]


def init_spark(args):
    # set_path
    spark_home = args['spark_home']
    os.environ['SPARK_HOME'] = spark_home
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    os.environ['PYSPARK_PYTHON'] = sys.executable
    sys.path.insert(0, os.path.join(spark_home, 'python'))
    sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.4-src.zip'))

    # print(sys.path)
    from pyspark import SparkContext, SparkConf, HiveContext
    conf = SparkConf().setAppName("name"). \
        setMaster("yarn-client"). \
        set('spark.local.dir', 'sparktmp'). \
        set('spark.executor.memory', '4g'). \
        set('spark.executor.cores', '1'). \
        set('spark.executor.instances', '6'). \
        set('spark.sql.parquet.mergeScheme', 'false'). \
        set('parquet.enable.summary-metadata', 'false'). \
        set('spark.yarn.executor.memoryOverhead', '6048mb'). \
        set('spark.driver.memory', '20g'). \
        set('spark.driver.maxResultSize', '40g'). \
        set('spark.yarn.driver.memoryOverhead', '6048mb'). \
        set('spark.port.maxRetries', '150'). \
        set('spark.dynamicAllocation.enabled', 'false'). \
        set('spark.kryoserializer.buffer.max.mb', '1g'). \
        set('spark.core.connection.ack.wait.timeout', '800s'). \
        set('spark.akka.timeout', '800s'). \
        set('spark.storage.blockManagerSlaveTimeoutMs', '800s'). \
        set('spark.shuffle.io.connectionTimeout', '800s'). \
        set('spark.rpc.askTimeout', '800s'). \
        set('spark.network.timeout', '800s'). \
        set('spark.rpc.lookupTimeout', '800s'). \
        set('spark.sql.autoBroadcastJoinThreshold', -1)

    sc = SparkContext.getOrCreate(conf)
    spark = HiveContext(sc)
    return sc, spark


def map_k_v(type, i):
    i += 1
    n_key = "--" * i + "key"
    t_key = get_type(type.keyType)
    i += 1
    n_value = "--" * i + "value"
    t_value = get_type(type.valueType)
    return [n_key, n_value], [t_key, t_value], i


def map(type_field, i, names, types):
    names_k_v, types_k_v, i = map_k_v(type_field, i)
    names.extend(names_k_v)
    types.extend(types_k_v)
    fields_value = type_field.valueType
    fields_map = None
    if isinstance(fields_value, ArrayType):
        elements = fields_value.elementType
        if isinstance(elements, MapType):
            map(fields_value.elementType, i, names, types)
        elif isinstance(elements, ArrayType):
            array(fields_value.elementType, i, names, types)
        elif isinstance(elements, StructType):
            fields_map = fields_value.elementType.fields
    elif isinstance(fields_value, StructType):
        fields_map = fields_value.fields
    elif isinstance(fields_value, MapType):
        map(fields_value, i, names, types)
    if fields_map:
        for el in fields_map:
            get_field_name_type(el, i + 1, names, types)


def struct(type_field, i, names, types):
    for el in type_field.fields:
        get_field_name_type(el, i + 1, names, types)


def array(type_field, i, names, types):
    fields_array = type_field.elementType
    if isinstance(fields_array, StructType):
        struct(fields_array, i, names, types)
    elif isinstance(fields_array, MapType):
        map(fields_array, i, names, types)
    elif isinstance(fields_array, ArrayType):
        array(fields_array, i, names, types)


def get_type(type):
    if isinstance(type, MapType):
        return "map" + "<" + type.keyType.typeName() + "," + get_type(type.valueType).split("<")[0] + ">"
    elif isinstance(type, ArrayType):
        return "array" + "<" + type.elementType.typeName() + ">"
    else:
        return type.typeName()


def get_field_name_type(field, i=0, names=None, types=None):
    if types is None:
        types = []
    if names is None:
        names = []
    name_field = "--" * i + field.name
    names.append(name_field)
    type_field = field.dataType
    types.append(get_type(type_field))

    if isinstance(type_field, StructType):
        struct(type_field, i, names, types)
    elif isinstance(type_field, ArrayType):
        array(type_field, i, names, types)
    elif isinstance(type_field, MapType):
        map(type_field, i, names, types)
    return names, types


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="""""")
    parser.add_argument('--spark_home', type=str, required=True,
                        help='Путь до spark. Например /opt/spark22/SPARK2-2.2.0.cloudera2-1.cdh5.12.0.p0.232957/lib/spark2')
    parser.add_argument('--excel_path', default='./Tables.xlsx', help='Путь до excel файла')
    parser.add_argument('--sheet_name', default=0, help='Название листа в excel файле')

    args = vars(parser.parse_args())

    df = pd.read_excel(args['excel_path'], sheet_name=args["sheet_name"])[1:]

    folder = 'all_files'
    archive(folder)

    sc, spark = init_spark(args)

    from pyspark.sql.catalog import Catalog
    from pyspark.sql.types import StructType, MapType, ArrayType

    catalog = Catalog(spark.sparkSession)

    # MAIN PART

    info_primitive = []
    info_composite = []
    excepts = []

    for index, row in df.iterrows():
        try:
            schema = row['T-schema'].strip()
            table = row['T-name'].strip()
            # cols = catalog.listColumns(table, schema)
            cols = spark.table('{}.{}'.format(schema, table)).schema.fields
            composite_type = [col.dataType for col in cols if col.dataType.simpleString().startswith(("struct", "array", "map"))]
            if composite_type != []:
                for col in cols:
                    names, types = get_field_name_type(col)
                    for n, t in zip(names, types):
                        info_composite.append(tuple([schema, table, n, t]))
            else:
                for col in cols:
                    info_primitive.append(tuple([schema, table, col.name, col.dataType.typeName()]))

        except Exception as e:
            excepts.append(str(e))

    # HDFS_PATH

    hdfs_path_schema = {}
    excepts_hdfs_path_schema = set()
    for index, row in df.iterrows():
        try:
            schema = row['T-schema'].strip()
            table = row['T-name'].strip()

            if schema not in hdfs_path_schema.keys():
                hdfs_path_schema[schema] = 'Look for this table in log'
                excepts_hdfs_path_schema.add(schema)
            if hdfs_path_schema[schema] == 'Look for this table in log':
                hdfs_path = get_hdfs_hive_table_path(spark, schema + '.' + table, folder)
                if hdfs_path != '':
                    path_table = hdfs_path.split('/')
                    if 'load' in path_table[-1]:
                        hdfs_path_schema[schema] = '/'.join(hdfs_path.split('/')[:-2])
                    else:
                        hdfs_path_schema[schema] = '/'.join(hdfs_path.split('/')[:-1])
                    excepts_hdfs_path_schema.remove(schema)
        except Exception as e:
            excepts.append(str(e))

    # SIZE

    size = []
    for index, row in df.iterrows():
        try:
            schema = row['T-schema'].strip()
            table = row['T-name'].strip()

            hdfs_path = get_hdfs_hive_table_path(spark, schema + '.' + table, folder)
            size_bytes = table_size(hdfs_path)
            size.append(size_bytes) if size_bytes != '' else size.append('нет доступа')
        except Exception as e:
            size.append('Look for this table in log')
            excepts.append(str(e))

    df['size(bytes)'] = size

    info_primitive = pd.DataFrame(info_primitive, columns=['Schema', 'Table', 'Field', 'Type'])
    info_primitive['Algorithm'] = ''
    info_primitive['Code'] = ''

    info_composite = pd.DataFrame(info_composite, columns=['Schema', 'Table', 'Field', 'Type'])
    info_composite['Algorithm'] = ''
    info_composite['Code'] = ''

    shema_hdfs = pd.DataFrame.from_dict(hdfs_path_schema, orient='index')
    shema_hdfs['Schema'] = shema_hdfs.index
    shema_hdfs.columns = ['hdfs_path', 'Schema']

    now = time.strftime('%Y_%m_%d_%H_%M')

    except_info = []
    shema_hdfs.to_excel(folder + '/Schema_HDFS_path_{now}.xlsx'.format(now=now), index=False)
    df.to_excel(folder + '/Tables_{now}.xlsx'.format(now=now), index=False)
    if info_primitive.shape[0] != 0:
        info_primitive.to_excel(folder + '/Tables_with_primitive_fields_{now}.xlsx'.format(now=now), index=False)
    else:
        except_info.append("Tables with primitive type of fields were not found!")
    if info_composite.shape[0] != 0:
        info_composite.to_excel(folder + '/Tables_with_composite_fields_{now}.xlsx'.format(now=now), index=False)
    else:
        except_info.append("Tables with composite type of fields were not found!")


    with open(folder + "/log_{now}.log".format(now=now), "w") as file:
        file.write("\n".join(excepts))
        file.write('\n\n\n\n\nDid not found path in hdfs for schemes: \n')
        file.write("\n".join(excepts_hdfs_path_schema))
        file.write("\n\n\n\n\n" + "\n".join(except_info))
