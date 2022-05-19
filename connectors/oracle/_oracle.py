from connectors.sqlalchemy import sqlalchemy

class connection(sqlalchemy):
    def __init__(self, conn_str, file_name, encoding, delimiter, batch, enable_parallel): 
        self.__conn_str=conn_str
        self.__batch=batch
        super().__init__('oracle', conn_str, file_name, encoding, delimiter, batch)  

    def read(self, query='', mode='overwrite',p_count=4,t_count=4):     
        try:
            yield from super().read(query=query, mode=mode)        
        except BaseException as e :
            super().print_messages(5000,str(e))

    def write(self, table_name, data=None):        
        try:
            super().write(table_name=table_name,  data=data)
        except BaseException as e:
            super().print_messages(5000,str(e))  
     
    def execute(self, query):
        try:
            super().execute(query)
        except BaseException as e:
            super().print_messages(5000,str(e))                      
   