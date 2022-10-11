import configparser
from log import i_messages
from connectors.file import file
from connectors.oracle import connection as oracle
from connectors.kafka import connection as kafka
from connectors.postgresql import connection as postgresql
from connectors.saphana import connection as saphana
from connectors.mysql import connection as mysql
from connectors.mssql import connection as mssql
from connectors.elasticsearch import connection as elasticsearch
import pickle
import pandas as pd

class connection_config(i_messages):
    def __init__(self, args): 

        i_messages.__init__(self, log_level=args.log_level)
        self.args = args
        
        self.conn=None
        self.target_conn=None
        self.data = None

        self.db = configparser.ConfigParser()
        self.db.read('./config/db.ini') 

        self.config = configparser.ConfigParser()
        self.config.read('./config/config.ini')
        
        self.check_args()
   
    def check_args(self):
         
        for arg in vars(self.args):
            if arg == 'query':
                self.query = self.args.query.replace("'","\'").replace('"','\"') if self.args.query is not None else 'None'
                continue
            exec("self."+arg+'='+"'"+str(getattr(self.args, arg))+"'")
        if(self.query =='None'):
            file_query = open("./config/query.sql", "r")
            self.query = file_query.read()
            
        self.conn_info = dict(self.config.items(self.environment)) 
        self.type=self.conn_info['type']
            
        if(self.type in  ['oracle','postgresql','mysql','mssql','saphana']):  
            if (self.statement in ['file2db','f2d']):
                if (self.query !='None'):
                     self.print_messages(5001)
                if (self.table_name =='None'):
                     self.print_messages(5201)
                if (self.file_name =='None'):
                     self.print_messages(5103)                         
                    
            if (self.statement in ['db2file','d2f','None','','execute','e']):
                if (self.query == 'None'):
                    self.print_messages(1103)
                    self.print_messages(1001)            
                
            if (self.statement in ['db2db','d2d']):
                if (self.table_name =='None'):
                    self.print_messages(5201)
                if (self.target_environment =='None'):
                    self.print_messages(5202)
                self.target_conn_info = dict(self.config.items(self.target_environment)) 
                self.target_type = self.target_conn_info['type']
                self.target_conn = self.connect(self.target_conn_info,self.target_type)
                if self.target_conn is None:
                    self.print_messages(5401)
            self.uniq_col = self.conn_info['uniq_col'] if 'uniq_col' in self.conn_info else 'None'
            self.query= '''{}'''.format(self.query)
                  
            if (self.enable_parallel!='None' and self.batch == 'None'):
                self.print_messages(5005)     
                
        self.encoding         = 'utf8' if self.encoding        == 'None' else self.encoding
        self.delimiter        = '¨'    if self.delimiter       == 'None' else self.delimiter
        self.batch            = -1     if self.batch           == 'None' else int(self.batch)
        self.enable_parallel  = False  if self.enable_parallel == 'None' else self.enable_parallel
        self.process_count    = 4      if self.process_count   == 'None' else int(self.process_count)
        self.thread_count     = 4      if self.thread_count    == 'None' else int(self.thread_count)        
        
        if (self.enable_parallel!='None' and self.statement =='file2db'):        
            self.__rc=file(file_name=self.file_name, encoding=self.encoding, delimiter=self.delimiter).row_count()-1 #Header -1
            self.__k,self.__l = divmod(self.__rc,(self.process_count*self.thread_count))
            self.__m,self.__n = divmod(self.__k,self.batch)
            if self.__m==0:
                super().print_messages(5004)            
        
        
        self.conn = self.connect(self.conn_info,self.type)  
        
    def connect(self,conn_info,conn_type):        
        try:  
            conn_str = self.db[conn_type]['conn_str'].format(**conn_info)
            #self.conn_params eklenebilir es ve kafka için
            
            params = {
              'conn_str':conn_str,
              'file_name':self.file_name,
              'encoding':self.encoding,
              'delimiter':self.delimiter,
              'batch':self.batch,
              'enable_parallel':self.enable_parallel
            }
            
            if (conn_type.lower() =='oracle'):
                conn = oracle(**params)
                
            elif (conn_type.lower()  == 'postgresql'): 
                conn = postgresql(**params)
            
            elif (conn_type.lower()  == 'saphana'): 
                conn = saphana(**params)
                
            elif (conn_type.lower()  == 'mysql'):
                conn = mysql(**params)
            
            elif (conn_type.lower()  == 'mssql'):
                conn = mssql(**params)

            elif (conn_type.lower()  == 'elasticsearch'):
                conn = elasticsearch(**params)
    
            elif (conn_type.lower()  == 'kafka'): 
                for key, value in conn_info.items():
                    try:
                        conn_info[key] = int(value)
                    except ValueError:
                        conn_info[key] = value if value!='None' else None
                conn = kafka(conn_info, topic=self.topic, file_name=self.file_name, encoding=self.encoding, delimiter=self.delimiter)
            return conn                
        except BaseException as e :
            self.print_messages(5000,str(e))   
            
