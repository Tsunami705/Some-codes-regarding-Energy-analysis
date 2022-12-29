#!/usr/bin/env python3

import sys, re, phoenixdb as phdb

sys.path.append('/home/node/proj/hbaseHttp/bin')

from conf import ApiConf
from threading import Lock
from dbutils.pooled_db import PooledDB

class Pyph():
     '''Database interface '''
     TEST_DB = 'select 1'
     LIST_TB = '''select distinct TABLE_SCHEM as "database", TABLE_NAME as "table name"
                  from SYSTEM."CATALOG"'''
     SEMI_REG = re.compile(r"(?:[^;']|'[^']*')+")
     POOL = PooledDB(phdb, url=ApiConf.PH_URL, autocommit=True)
    
     def __init__(self):
         self. lock = Lock()
    
     def tb_sql(self, params):
         ''' Execute SQL statement
        
             parameter:
                 sql: SQL statement
            
             return:
                 JSON String result set
         '''
         if isinstance(params, str):
             sql = params
         else:
             sql = params['sql']
        
         rs = ''
         sql = Pyph. SEMI_REG. findall(sql)
         num = len(sql)
         with Pyph.POOL.connection() as conn:
             self. lock. acquire()
             with conn.cursor(cursor_factory=phdb.cursor.DictCursor) as cur:
                 for each in sql:
                     if each != '':
                         try:
                             cur. execute(each. strip())
                         except Exception as err:
                             num -= 1
                             ApiConf().get_file_logger('pyph') \
                                 .error(f"[{err}] {each}")
                 try:
                     rs = cur. fetchall()
                 except Exception as err:
                     rs = f'{num} statements executed successfully: ({sql[0]})...'
             self. lock. release()
        
         del sql
         del params
         return rs
    
     def db_test(self, params = None):
         '''Test database connection '''
         return self.tb_sql(Pyph.TEST_DB)
    
     def list_tb(self, params = None):
         ''' list all tables '''
         return self.tb_sql(Pyph.LIST_TB)
    
     def des_tb(self, params):
         ''' view table properties
        
             parameter:
                 table: table name
         '''
         sql = f"""select COLUMN_NAME as "column name",
                          SqlTypeName(DATA_TYPE) as "data type"
                   from SYSTEM."CATALOG"
                   where TABLE_NAME = '{params['table']}'
                   and COLUMN_NAME IS NOT NULL"""
         return self.tb_sql(sql)
    
     def drop_tb(self, params):
         ''' delete table
        
             parameter:
                 table: table name
         '''
         return self.tb_sql(f"drop table if exists {params['table']}")
    
     def create_tb(self, params):
         ''' Create a new table
        
             Create tables by specifying table names, column names, data types, and constraints
            
             parameter:
                 table: table name
                 columns: "column name": "data type" column dictionary
                 constraints: "Constraint type": ["column name",...] constraint dictionary
                
             return value:
                 JSON String result set
         '''
         columns = params['columns']
         constraints = {
             key.lower():value for key,value in params['constraints'].items()
         }
        
         for cname, ctype in columns.items():
             columns[cname] = f'{cname} {ctype}'
             if 'not null' in constraints and cname in constraints['not null']:
                 columns[cname] += 'not null'
                
         cols = ','.join([columns.values()])
         pk = constraints['primary key']
         sql = f"""create table if not exists {table}(
                       {col}, constarint PK_{table} primary key({pk})
                   )COLUMN_ENCODED_BYTES=0, SALT_BUCKETS=3"""
        
         return self.tb_sql(sql)
    
     def del_tb(self, params):
         ''' Delete table data
        
             parameter:
                 table: table name
                 datas: "column name": "value" conditional dictionary
         '''
         datas = params['datas']
         for name, value in datas.items():
             if isinstance(value,str):
                 datas[name] = f"'{value}'"
             datas[name] = f"{name}={value}"
         datas = ' and '.join([datas.values()])
        
         return self.tb_sql(f"delete from {params['table']} where {datas}")
    
     def upsert_tb(self, params):
         ''' Update table data
        
             parameter:
                 table: table name
                 datas: "column name": "value" data dictionary
         '''
         datas = params['datas']
         for name, value in datas.items():
             if isinstance(value,str):
                 datas[name] = f"'{value}'"
        
         return self.tb_sql(
             f"""upsert into {params['table']}({','.join([datas.keys()])})
                 values({','.join([datas.values()])})"""
         )