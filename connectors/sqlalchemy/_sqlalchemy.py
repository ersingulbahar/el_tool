import sqlalchemy as sql
import pandas as pd
from log import i_messages
from connectors.file import file
import numpy as np


class sqlalchemy(i_messages):
    def __init__(self, db_type, conn_str, file_name, encoding, delimiter, batch):
        i_messages.__init__(self)
         
        self.__execute_cursor = None
        self.__data = None
        self.__columns = None
        self.__file_name = file_name
        self.__encoding = encoding
        self.__db_type = db_type
        self.__delimiter = delimiter
        self.__batch = batch
        self.__conn_str=conn_str
        self.__engine = sql.create_engine(self.__conn_str, connect_args={"encoding": "UTF-8","nencoding": "UTF-8"} if self.__db_type  in ['oracle','file'] else {} ) 
        self.__conn = self.__engine.raw_connection()
                
    def file_mode_operations(self,query,mode,columns = None):
        
        if columns is not None:
            if mode is None or mode !='a': 
                file(file_name=self.__file_name).delete_file()
                file(file_name=self.__file_name,data=pd.DataFrame(np.array([columns])), encoding=self.__encoding, delimiter=self.__delimiter).create_file_with_header() 
        else:
            __engine = sql.create_engine(self.__conn_str, connect_args={"encoding": "UTF-8","nencoding": "UTF-8"} if self.__db_type in ['oracle','file'] else {}) 

          #  query = query + ' where rownum=1'  

            __execute_cursor = __engine.execution_options(stream_results=True).execute(query)
            __columns = [col for col in __execute_cursor.keys()]

            if mode is None or mode !='a': 
                file(file_name=self.__file_name).delete_file()
                file(file_name=self.__file_name,data=pd.DataFrame(np.array([__columns])), encoding=self.__encoding, delimiter=self.__delimiter).create_file_with_header()   

            if(__execute_cursor):
                __execute_cursor.close()
    
    
    def read(self, query, mode):
        try:
            self.print_messages(1201,self.__db_type)
              
            if self.__batch == -1:
                self.__execute_cursor = self.__engine.execute(query)
                self.__columns = [col for col in self.__execute_cursor.keys()]
                result = self.__execute_cursor.fetchall() 
                self.__data = pd.DataFrame(result,columns=self.__columns) 
                self.print_messages(1001) 
                self.file_mode_operations(query,mode,self.__columns)
                yield self.__data
            else:
                self.__execute_cursor = self.__engine.execution_options(stream_results=True).execute(query)
                self.__columns = [col for col in self.__execute_cursor.keys()]
                self.file_mode_operations(query,mode,self.__columns)
                while 'batch not Empty': 
                    try :
                        result = self.__execute_cursor.fetchmany(size=self.__batch)
                    except:
                        result = False

                    if(not result):
                        break
             
                    self.__data = pd.DataFrame(result,columns=self.__columns) 
                    self.print_messages(1001)

                    yield self.__data
                
        except BaseException as e :
            self.print_messages(5000,str(e))
        finally:
            self.close_cursor()
            self.close_connection() 
    
    def write_mode(self, table_name, mode):
        if mode in ("overwrite",'w'):
            self.execute(f"truncate table {table_name}")
            
    def write(self, table_name, data):
        self.__data = data
        try:
            self.__execute_cursor = self.__conn.cursor() 
            self.print_messages(1211,self.__db_type)

            rows = [tuple(x) for x in self.__data.replace({np.nan: None}).values]  
            column_names, column_values = self.get_col_names_and_values() 

            query = 'INSERT INTO ' + table_name.upper() + ' ' + column_names + ' VALUES ' + column_values   
            if self.__db_type=='oracle' :
                self.__execute_cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'")
            self.__execute_cursor.executemany(query, rows)
            self.__conn.commit()  
            self.print_messages(1001)
        except BaseException as e:
            self.print_messages(5000,str(e))
        finally:
            self.close_cursor() 
                
    def execute(self, query):
        try:
            self.print_messages(1221,self.__db_type)
            self.__engine.execute(query)
            self.print_messages(1001)
        except BaseException as e:
            self.print_messages(5000,str(e))
                
    def get_col_names_and_values(self):
        column_names = ''
        column_values = ''
        for column in self.__data.columns:
            column_names += str(column) + ','
            if self.__db_type=='oracle':
                column_values += ':' + str(column) + ','
            elif self.__db_type in ['postgresql','mysql']:
                column_values += '%s,'    
            elif self.__db_type in ['mssql','saphana']:
                column_values += '?,'                 
        return '(' +  column_names[:-1]+ ')', '(' +  column_values[:-1]+ ')' 
    
    def close_cursor(self): 
        if (self.__execute_cursor):
            self.__execute_cursor.close()  
            
    def close_connection(self):
        if (self.__conn):
            self.__conn.close()