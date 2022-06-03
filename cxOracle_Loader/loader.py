# -*- coding: utf-8 -*-
#!/home/ektov-av/bin/python35
import os
import sys
os.environ['NLS_LANG'] = 'RUSSIAN_RUSSIA.AL32UTF8'
import pandas as pd
import time
import datetime
import joblib
from joblib import Parallel, delayed
from collections import OrderedDict
import cx_Oracle

class OracleDB(object):

    def __init__(self, user, password, sid):
        self.user = user
        self.password = password
        self.sid = sid
        self.DatabaseError = cx_Oracle.DatabaseError
        self.connection    = cx_Oracle.connect(self.user, self.password, self.sid)

    def connect(self):
        self.connection = cx_Oracle.connect(self.user, self.password, self.sid)
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

class Loader(object):
    """
    Class contains functions for loading data from database.
    ------------------------------------------------------------------------------------
    Functions:
        get_dataframe      - return dataframe from sql query
        get_dataframe_clob - return dataframe from sql query; use this function
                             in the case of existence of CLOB objects in returingn db.
                             During checking of CLOB instance all values in CLOB columns
                             will be casted to type 'str'
        save_csv           - fetch db using sql query and save it to csv according
                             to specified local path

        upload_csv         - upload csv to iskra
    -----------------------------------------------------------------------------------
    """
    def __init__(self, init_dsn=False, encoding='cp1251', sep=','):
        self.tns_names = {
        'sasprod': cx_Oracle.makedsn('', 1521, $HOSTNAME),
        'iskra4' : cx_Oracle.makedsn('',   1521, $HOSTNAME),
        'iskra3' : cx_Oracle.makedsn('',  1521, $HOSTNAME),
        'iskra2' : cx_Oracle.makedsn('',  1521, $HOSTNAME),
        'iskra1' : """(DESCRIPTION =
           (LOAD_BALANCE=off)
             (FAILOVER = ON)
             (ADDRESS_LIST =
                  (ADDRESS = (PROTOCOL = TCP)  (HOST = $HOSTNAME) (PORT = 1521) )
                  (ADDRESS = (PROTOCOL = TCP)  (HOST = $HOSTNAME) (PORT = 1521) )
                  )
             (CONNECT_DATA = (SERVICE_NAME = cskostat_primary) (FAILOVER_MODE= (TYPE=select) (METHOD=basic)
                  )
                 ))"""
         }
        self.init_dsn    = init_dsn
        self.encoding    = encoding
        self.sep         = sep

    def _get_dsn(self, iskra):
        return self.tns_names[iskra]

    def get_dataframe(self, query, login='', iskra=$HOSTNAME, password=''):
        """
        Return dataframe  for specified query.

        Args:
            query (str): sql query
            iskra (str): from which iskra get table
        Returns:
            pandas.DataFrame: dataframe  for specified query
        """
        try:
            if self.init_dsn:
                conn = cx_Oracle.connect(login, password, self._get_dsn(iskra))
            else:
                conn = cx_Oracle.connect('{0}/{1}@{2}'.format(login, password, iskra))
            df = pd.read_sql(query, con=conn)
            df.columns = map(str.lower, df.columns)
            return df
        except Exception as e:
            print(str(e))

    def get_dataframe_clob(self, query, iskra=$HOSTNAME, password=''):
        """
        Return dataframe  for specified query.

        Args:
            query (str): sql query
            iskra (str): from which iskra get table
        Returns:
            pandas.DataFrame: dataframe for specified query
        """
        def fix_clob(row):
            def _convert(col):
                if isinstance(col, cx_Oracle.LOB) or isinstance(col, cx_Oracle.CLOB):
                    return str(col)
                else:
                    return col
            return [_convert(c) for c in row]

        try:
            if self.init_dsn:
                db = OracleDB('', password, self._get_dsn(iskra))
            else:
                db = OracleDB('', password, iskra)
            print('Connecting...')
            db.connect()
            db.cursor.execute(query)

            #df = pd.read_sql(query, con=conn)
            #df.columns = map(str.lower, df.columns)
            columns = [x[0].lower() for x in db.cursor.description]
            return pd.DataFrame([fix_clob(row) for row in db.cursor], columns = columns)
        except Exception as e:
            print(str(e))


    def _read_from_db(self, db, columns, file_name, append_header,compress):
        rows = db.cursor.fetchmany()
        if not rows:
            return False

        data = pd.DataFrame(rows, columns=columns)
        if compress:
            data.to_csv(file_name, mode='a', index=False, compression='gzip',
                        header=append_header, sep=self.sep,  encoding=self.encoding)
        else:
            data.to_csv(file_name, mode='a', index=False,
                        header=append_header, sep=self.sep,  encoding=self.encoding)
        append_header = False

        return True

    def _corpora_iter(self, df, num_partitions=15):
        len_batches = len(df) // num_partitions
        if len(df) % num_partitions:
            num_partitions+=1
        for i in range(num_partitions):
            __start = i*len_batches
            __end   = min((i+1)*len_batches,len(df))
            yield df.loc[__start:__end-1,:]


    def _commitrows_many(self, args):
        df, db, sql = args
        db.connect()
        rows = df.values.tolist()
        db.cursor.executemany(sql, rows)
        db.connection.commit()
        print('Pushed {} lines '.format(len(df)))
        db.close()


    def _commitrows(self, args):
        df, db, sql, isclobe = args
        db.connect()
        #rows = df.values.tolist()
        if isclobe:
            rows = df.to_dict('records') #.values.tolist()
        else:
            rows = df.values.tolist()

        for row in rows:
            db.cursor.execute(sql, row)
        # db.cursor.executemany(sql, rows)
        db.connection.commit()
        print('Pushed {} lines '.format(len(df)))
        db.close()

    def _push_balance_parallel(self, db, df, table_name, path, verbose, compress,
                               isclobe, isuseclobdct, isallvarchar, njobs):

        if path is not None:
            with open(path) as f:
                if not isinstance(df, pd.DataFrame):
                    print('Fetching dataframe from csv... ')
                    if compress:
                        reader = pd.read_csv(path, sep=self.sep,  encoding=self.encoding,
                                             compression='gzip', na_values='NaN', na_filter=False)
                    else:
                        reader = pd.read_csv(path, sep=self.sep,  encoding=self.encoding,
                                             na_values='NaN', engine='python', na_filter=False)
        else:
            if isinstance(df, pd.DataFrame):
                reader = df

            if isallvarchar:
                reader = reader.where((reader.notnull()),None)
                reader.fillna('',inplace=True)
                reader = reader.astype(str)

                for col in reader.columns:
                    if reader[col].str.len().max() >= 34000:
                        reader[col] = reader[col].apply(lambda x: x[:34000])

            if isclobe:
                clobdct = OrderedDict()
                con = db.connection
                col=pd.read_sql("select column_name, data_type from user_tab_columns where lower(table_name) like '%s' order by column_id" % table_name.lower(), con=con)
                for col,__type in zip(col['COLUMN_NAME'].values,col['DATA_TYPE'].values):
                    if __type == 'VARCHAR2':
                        clobdct[col.upper()] = cx_Oracle.LONG_STRING
                    elif __type == 'CLOB':
                        clobdct[col.upper()] = cx_Oracle.CLOB
                    elif __type == 'TIMESTAMP(6)':
                        clobdct[col.upper()] = cx_Oracle.TIMESTAMP
                    elif __type == 'NUMBER':
                        clobdct[col.upper()] = cx_Oracle.NUMBER
                print(clobdct)
                con.cursor().setinputsizes(**clobdct)

                sortedtpl = sorted([(col,clobdct[col.upper()] == cx_Oracle.CLOB) for col in reader.columns.values], key=lambda x: x[1])
                sortedcols = [col for col,__bool in sortedtpl]
                reader = reader[sortedcols]

            columns_str= ', '.join(reader.columns.values).upper()
            if isuseclobdct:
                formatstr=','.join([':'+str(i.upper()) for i in (reader.columns.values)])
            else:
                formatstr=','.join([':'+str(i) for i in range(len(reader.columns.values))])
            substr = "INSERT INTO {} ({}) VALUES "+"("+formatstr+")"
            sql = substr.format(table_name, columns_str)

            print(sql)

            print('Pushing data to iskra ... ')

            Parallel(n_jobs=njobs, verbose=2) ( map(delayed(self._commitrows), [(__iter, db, sql, isclobe) for __iter in self._corpora_iter(reader, njobs)]  ) )


    def _push_rows_parallel(self, db, df, table_name, verbose, njobs):

        print('Fetching dataframe from csv... ')
        columns_str= ', '.join(df.columns.values).upper()
        formatstr=','.join([':'+str(i) for i in range(len(df.columns.values))])
        substr = "INSERT INTO {} ({}) VALUES "+"("+formatstr+")"
        sql = substr.format(table_name, columns_str)
        print(sql)
        print('Pushing data to iskra ... ')

        Parallel(n_jobs=njobs, verbose=verbose) ( map(delayed(self._commitrows_many), [(__iter, db, sql) for __iter in self._corpora_iter(df, njobs)]  ) )


    def _push_balance(self, db, df, table_name, path, verbose, compress, isclobe, isuseclobdct, isallvarchar):

        print('Connecting...')
        db.connect()
        start_time = time.time()

        if path is not None:
            with open(path) as f:
                if not isinstance(df, pd.DataFrame):
                    print('Fetching dataframe from csv... ')
                    if compress:
                        reader = pd.read_csv(path, sep=self.sep,  encoding=self.encoding, compression='gzip', na_values='NaN', engine='python', na_filter=False)
                    else:
                        reader = pd.read_csv(path, sep=self.sep,  encoding=self.encoding, na_values='NaN', engine='python', na_filter=False)
        else:
            if isinstance(df, pd.DataFrame):
                reader = df

        print("Total number of records to be inserted: {}".format(df.shape[0]))

        if isallvarchar:
            reader = reader.where((reader.notnull()),None)
            reader.fillna('',inplace=True)
            reader = reader.astype(str)

            for col in reader.columns:
                if reader[col].str.len().max() >= 34000:
                    reader[col] = reader[col].apply(lambda x: x[:34000])

        if isclobe:
            clobdct = OrderedDict()
            con = db.connection
            col=pd.read_sql("select column_name, data_type from user_tab_columns where lower(table_name) like '%s' order by column_id" % table_name.lower(), con=con)
            for col,__type in zip(col['COLUMN_NAME'].values,col['DATA_TYPE'].values):
                if __type == 'VARCHAR2':
                    clobdct[col.upper()] = cx_Oracle.LONG_STRING
                elif __type == 'CLOB':
                    clobdct[col.upper()] = cx_Oracle.CLOB
                elif __type == 'TIMESTAMP(6)':
                    clobdct[col.upper()] = cx_Oracle.TIMESTAMP
                elif __type == 'NUMBER':
                    clobdct[col.upper()] = cx_Oracle.NUMBER
            print(clobdct)
            con.cursor().setinputsizes(**clobdct)

            sortedtpl = sorted([(col,clobdct[col.upper()] == cx_Oracle.CLOB) for col in reader.columns.values], key=lambda x: x[1])
            sortedcols = [col for col,__bool in sortedtpl]
            reader = reader[sortedcols]

        columns_str= ', '.join(reader.columns.values).upper()
        if isuseclobdct:
            formatstr=','.join([':'+str(i.upper()) for i in (reader.columns.values)])
        else:
            formatstr=','.join([':'+str(i) for i in range(len(reader.columns.values))])
        substr = "INSERT INTO {} ({}) VALUES "+"("+formatstr+")"
        sql = substr.format(table_name, columns_str)

        print(sql)

        print('START export ... ')

        rows_per_execute= 50000
        n_samples = len(reader)
        n_batches = n_samples // rows_per_execute
        if n_samples % rows_per_execute:
            n_batches+=1

        print('Pushing data to iskra ... ')
        rows = []
        for i in range(n_batches):
            batch_start = i * rows_per_execute
            batch_end   = min((i+1)*rows_per_execute,n_samples)
            if isclobe:
                rows = reader.loc[batch_start:batch_end,:].to_dict('records') #.values.tolist()
            else:
                rows = reader.loc[batch_start:batch_end,:].values.tolist()

            #print(rows)
            #if i == 0:
            #    continue
            #rows.append(row)
            #if i and i % rows_per_execute == 0:

            #db.cursor.executemany(sql, rows)
            for row in rows:
                db.cursor.execute(sql, row)
            db.connection.commit()
            rows = []
            if verbose == 1:
                print('Pushed {:,} lines, {:7.0f}sec. passed'
                      .format(batch_end,time.time() - start_time))

        db.close()

    def _get_balance(self, db, sql, file_name, verbose, compress):
        print('Connecting...')
        db.connect()

        start_time = datetime.datetime.now()
        print('Getting data ... ')
        db.cursor.arraysize = 10000

        db.cursor.execute(sql)
        columns = [x[0].lower() for x in db.cursor.description]

        start_time = time.time()
        append_header = True

        if os.path.exists(file_name):
            os.remove(file_name)

        count = 0
        while True:
            result = self._read_from_db(db, columns, file_name, append_header, compress)
            count += 1
            append_header = False
            if not result:
                break
            if verbose == 1:
                print('Downloaded {:,} lines, {:7.0f}sec. passed'
                      .format(count * db.cursor.arraysize,
                              time.time() - start_time))
        db.close()

    def save_csv(self, query, path='data.csv', iskra=$HOSTNAME, password='', verbose=1, compress=1):
        """
        Saves csv  for specified query.

        Args:
            query (str): sql query
            path (str): path
            iskra (str): from which iskra get table
        """
        try:
            if self.init_dsn:
                db = OracleDB('', password, self._get_dsn(iskra))
            else:
                db = OracleDB('', password, iskra)
            self._get_balance(db, query, path, verbose, compress)
        except Exception as e:
            print(str(e))

    def upload_df_or_csv(self, df, table_name, path='/data/data.csv',
                         iskra=$HOSTNAME, password='',
                         parallel = 0, njobs=None, verbose=1, compress=1,
                         isclobe = 0, isuseclobdct = 0, isallvarchar = 0):
        """
        Fetch db from csv and upload it according to the specified query.

        Args:
            query (str)          : sql query
            path (str)           : path
            iskra (str)          : from which iskra space to get table
            parallel (int)       : use parallel pooll for uploading
            njobs (int)          : num of workers and also a number of dataframe splits
            compress (int)       : how to read saved csv (if 'gzip' then set to 1 else 0)
            isclobe (int)        : set to 1 if we have a CLOB columns in a prepared iskra database
                                   otherwise ORA-01461 error will be raised:

                                        "Can bind a LONG value only for insert into a LONG column".

                                   Also if we gonna to upload a LONG/CLOB
                                   values into oracle CLOB columns one should to supply a dict with
                                   <cx_Oracle.CLOB> types for specific columns of type CLOB
                                   See conn.cursor().setinputsizes(**dct) method as implemented in code.
                                   In addition we have to re-order all columns in an uploaded dataframe
                                   so that the LONG bind or LOB binds are all at the end of the bind list.
                                   Otherwise we'll faced with the following ORA-24816 error:

                                        "Expanded non LONG bind data supplied after actual LONG or LOB column".

            isuseclobdct (int)   : set to 1 if we want to insert values into a table according to keys in
                                   prepared dictionary feeded to conn.cursor().setinputsizes(**dct) method
                                   (column's name --> dict key mapping). If parameter is set to 0 so positional
                                   mapping will be used for insert into.
        """
        try:
            if self.init_dsn:
                db = OracleDB('', password, self._get_dsn(iskra))
            else:
                db = OracleDB('', password, iskra)
            if not parallel:
                self._push_balance(db, df, table_name, path, verbose, compress, isclobe, isuseclobdct, isallvarchar)
            else:
                self._push_balance_parallel(db, df, table_name, path, verbose, compress,
                                            isclobe, isuseclobdct, isallvarchar, njobs)

        except Exception as e:
            print(str(e))


    def df2oracle(self, db, df, table_name, njobs=None, verbose=1):
        try:
          self._push_rows_parallel(db, df, table_name, verbose, njobs)
        except Exception as e:
          print(str(e))







