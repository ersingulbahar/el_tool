import pandas as pd
import threading
import multiprocessing as mp
from log import i_messages 
import sqlalchemy as sql
from connectors.sqlalchemy import sqlalchemy
from connectors.file import file

class parallel(i_messages):
    
    def __init__(self, conn_str,target_conn_str,db_type, batch, table_name, file_name, encoding, delimiter, mode, log_level, unique_column, uniq_col): 
        self.__file_name=file_name
        self.__conn_str=conn_str
        self.__target_conn_str=target_conn_str
        self.__db_type = db_type
        self.__batch = batch
        self.__table_name = table_name
        self.__encoding=encoding
        self.__delimiter=delimiter
        self.__mode=mode
        self.__unique_column=unique_column
        self.__uniq_col=uniq_col
        i_messages.__init__(self, log_level=log_level)
    
    def process_run(self, x, query, p_count, t_count):  
        threads = [] 
        for y in range(t_count):
            thread = threading.Thread(target=self.thread_run, args=(x, y, query, p_count, t_count,))  
            threads.append(thread) 
            thread.start()
        for thread in threads:
            thread.join()           

    def thread_run(self,x, y, query, p_count,t_count):  
        __engine = sql.create_engine(self.__conn_str, 
                                       connect_args={"encoding": "UTF-8","nencoding": "UTF-8"} if self.__db_type in ['oracle','file'] else {} ) 
         
        
        where_clause = self.__uniq_col if self.__unique_column == 'None' else 'mod(to_number('+self.__unique_column+')'
        if (where_clause == 'None'): 
            self.print_messages(5006)  
        
            
        query = query + ' where '+ where_clause +','+str((p_count*t_count)-1)+') = ' + str(x+y*p_count)               
        self.print_messages(1000)
        
        __execute_cursor = __engine.execution_options(stream_results=True).execute(query)
        __columns = [col for col in __execute_cursor.keys()] 
        
        while True:
            try :
                result = __execute_cursor.fetchmany(size=self.__batch)
            except:
                result = []
            
            if(len(result)==0):
                break
            
            __data = pd.DataFrame(result,columns=__columns) 
            self.print_messages(1001) 
            
            if(self.__target_conn_str=="file"): 
                file(file_name=self.__file_name, data=__data, encoding=self.__encoding, delimiter=self.__delimiter, log_level=self.log_level).write()
            else:
                __sqlalchemy = sqlalchemy(self.__db_type, self.__target_conn_str,'','','','')
                __sqlalchemy.write(table_name=self.__table_name, data=__data)
            
        if(__execute_cursor):
            __execute_cursor.close() 
    
    def execute(self, query):
        try:
            self.execute(query)
        except BaseException as e:
            self.print_messages(5000,str(e))    

    
    def run_x(self, query='',p_count=4,t_count=4): 
        if mp.cpu_count()<p_count:
            p_count=mp.cpu_count() 
        try: 
            __file_mod = sqlalchemy(self.__db_type, self.__conn_str, self.__file_name, self.__encoding, self.__delimiter, self.__batch)
            __file_mod.file_mode_operations(query,self.__mode,None)
            processes = []            
            for x in range(p_count):
                p = mp.Process(target=self.process_run, args=(x, query, p_count, t_count, ))
                processes.append(p)
                p.start()                
            for p in processes:                 
                p.join()   
                        
        except BaseException as e:
            self.print_messages(5000,str(e))        
                    