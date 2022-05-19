from datetime import datetime
from elasticsearch import Elasticsearch
from log import i_messages
from connectors.file import file


class connection(i_messages):
    def __init__(self, conn_str, file_name, encoding, delimiter):
        i_messages.__init__(self) 
        self.__data = None
        self.__file_name = file_name
        self.__encoding  = encoding
        self.__delimiter = delimiter
        
        exec("self.es= Elasticsearch("+conn_str+")")
        doc = {'size' : 10000,'query': {'match_all' : {}}}

    def read(self, query='', mode='overwrite'):
        self.print_messages(1000,1008)
        self.data = self.es.search(index='gantt_tsk-2021.03', doc_type='_doc', body=doc,scroll='1m')
        print(self.data)
        exit()
        file(file_name=self.__file_name, data=self.__data, mode=mode, encoding=self.__encoding, delimiter=self.__delimiter).write()

            
    def write(self, table_name=''):
        exit()
        self.__data = file(file_name=self.__file_name, encoding=self.__encoding, delimiter=self.__delimiter).read()  
        data_list = self.__data.to_dict('records')
        
        self.print_messages(1000,1009)
         