import argparse
from utils.module import install_modules
from connection.engine import engine
from log import i_messages
import gc
import time
from argparse import Namespace

start_time = time.time()
log=i_messages()

class EL:
    def __init__(self, query ='None', environment ='None', target_environment ='None', statement ='None', file_name ='None', 
                     table_name ='None', mode ='None', topic ='None', encoding ='None', delimiter ='None', batch ='None', 
                     enable_parallel ='None', process_count ='None', thread_count ='None', unique_column ='None', log_level ='None'):
        self.query = query
        self.environment = environment
        self.target_environment = target_environment
        self.statement = statement
        self.file_name = file_name
        self.table_name = table_name
        self.mode = mode
        self.topic = topic
        self.encoding = encoding
        self.delimiter = delimiter
        self.batch = batch
        self.enable_parallel = enable_parallel
        self.process_count = process_count
        self.thread_count = thread_count
        self.unique_column = unique_column
        self.log_level = log_level
        self.args = Namespace()
        self.assign_args()

    def assign_args(self):
        for key, value in self.__dict__.items():
            if key != 'args':
                exec("self.args." + key + "='" + value + "'")
    
    def run(self):
        log.print_messages(1000,'************************EL************************')
        engine(self.args)
        gc.collect()
        log.print_messages(1001,'************************EL************************\nTotal run time:'+str(time.time() - start_time)+' seconds.')
 