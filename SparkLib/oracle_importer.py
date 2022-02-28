import datetime, sys, os

# Конфигурация для ЛД 2.0
spark_home = '/opt/cloudera/parcels/SPARK2_INCLUDE_SPARKR/lib/spark2'
# spark_home = '/home/korsakov.n.n/spark_220'
os.environ['SPARK_HOME'] = spark_home
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
os.environ['PYSPARK_PYTHON'] = '/opt/cloudera/parcels/PYENV.ZNO20008661/bin/python'

sys.path.insert(0, os.path.join (spark_home,'python'))
sys.path.insert(0, os.path.join (spark_home,'python/lib/py4j-0.10.4-src.zip'))

from connector import OracleDB
from formatter import colprint
import csv

class OracleImporter:

    def __init__(self, db, csv_file=None):
        self.db = OracleDB(db)
        self.db.connect()
        self.batch_size = 5000
        self.db.cursor.arraysize = self.batch_size
        self.csv_file = csv_file

    def sql (self, query, fieldnames):
        start_time = datetime.datetime.now()

        with open(self.csv_file, 'w', newline='') as fw:
            writer = csv.DictWriter(fw, fieldnames=fieldnames)
            writer.writeheader()
            self.db.cursor.execute(query)

            c = 0
            while True:
                rows = self.db.cursor.fetchmany()
                if not rows:
                    break
                for row in rows:
                    data_row = dict(zip(fieldnames, row))
                    writer.writerow(data_row)
                if c and c % 200 == 0:
                    colprint('{:13,d} rows [{}]'.format(c*self.batch_size, \
                        str(datetime.datetime.now() - start_time)[:-7]))
                c += 1

            self.db.close()
