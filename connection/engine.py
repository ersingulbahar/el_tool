from log import i_messages
from .connection_config import connection_config
from connectors.file import file
import pandas as pd
from connectors.sqlalchemy._parallel import parallel

class engine(connection_config):
    def __init__(self, args):
        connection_config.__init__(self, args)
        self.run()

    def run(self):
        if self.type.lower() in ('oracle','kafka','elasticsearch','postgresql','mssql','mysql','saphana') :
             
            if(self.enable_parallel):  
                if (self.statement.lower() in ('db2db','d2d')):
                    self.target_conn.write_mode(self.table_name, self.mode) 
                    target_conn_str = self.db[self.target_type]['conn_str'].format(**self.target_conn_info) 
                else:
                    target_conn_str = "file"
                    self.target_type = "file" 
                conn_str = self.db[self.type]['conn_str'].format(**self.conn_info)          
                par=parallel(conn_str,target_conn_str,self.target_type,self.batch,self.table_name,                                                                                                                            self.file_name,self.encoding,self.delimiter,self.mode,log_level=self.log_level,unique_column=self.unique_column,uniq_col=self.uniq_col )
                par.run_x(self.query,self.process_count,self.thread_count)   
            else:         
                if  self.statement.lower() in ('db2file','d2f'): 
                    self.data = self.conn.read(query=self.query, mode=self.mode,p_count=self.process_count,t_count=self.thread_count)
                    for data in self.data:
                        file(file_name=self.file_name, data=data, encoding=self.encoding, delimiter=self.delimiter).write()

                elif self.statement.lower() in ('file2db','f2d'): 
                    self.conn.write_mode(self.table_name, self.mode)
                    i=0
                    exit_=False
                    while(not exit_):
                        data = file(file_name=self.file_name, encoding=self.encoding, delimiter=self.delimiter).read(self.batch*i if self.batch!=-1 else -1,self.batch)
                        for data_ in data:
                            if len(data_)==0 :
                                exit_=True   
                                break
                            self.conn.write(table_name=self.table_name, data=data_)
                            if self.batch==-1:
                                exit_=True
                        i+=1
                    self.conn.close_connection() 
                elif self.statement.lower() in ('db2db','d2d'): 
                    self.target_conn.write_mode(self.table_name, self.mode)
                    data = self.conn.read(query=self.query, mode=self.mode,p_count=self.process_count,t_count=self.thread_count)
                    for data_ in data: 
                        if len(data_)==0 : 
                            break
                        self.target_conn.write(table_name=self.table_name, data=data_)
                    self.target_conn.close_connection() 
                elif self.statement.lower() in ('execute','e'):
                    self.conn.execute(self.query)
                else:
                    self.print_messages(5002)
        else:
            self.print_messages(5003)