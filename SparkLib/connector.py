import os, sys

sys.path.insert(0, '/opt/cloudera/parcels/PYENV.ZNO0059623792/usr/lib/oracle/12.2/client64/lib')
#import
#print(os.environ)
#import /opt/cloudera/parcels/PYENV.ZNO0059623792/usr/lib/oracle/12.2/client64/lib/
import cx_Oracle

class TeraDB:
    def __init__(self, HOST, DB, USERNAME, PASSWORD):
        self.DatabaseError = cx_Oracle.DatabaseError
        self.dsn = 'jdbc:teradata://{host}/database={db}, LOGMECH=LDAP, charset=UTF8,TYPE=FASTEXPORT,\
                                                                      COLUMN_NAME=ON, MAYBENULL=ON'.format(host=HOST, db=DB)
        self.user, self.password = USERNAME, PASSWORD


class OracleDB:

    def __init__(self, sid):
        self.DatabaseError = cx_Oracle.DatabaseError
        if sid == 'iskra1':
            self.dsn = '10.112.40.242:1521/cskostat_primary'
            self.user, self.password = '', ''
        elif sid == 'iskra2':
            self.dsn = '10.112.242.254:1521/iskra2_iskra2'
            self.user, self.password = '', ''
        elif sid == 'iskra3':
            self.dsn = '10.112.102.106:1521/iskra3'
            self.user, self.password = '', ''
        elif sid == 'iskra4':
            self.dsn = '10.112.79.164:1521/iskra4'
            self.user, self.password = '', ''
        elif sid == 'sasprod':
            self.dsn = 'kae9.ca.sbrf.ru:1521/sasprod'
            self.user, self.password = '', ''

    def connect(self):
        self.connection = cx_Oracle.connect(self.user, self.password, self.dsn)
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()
