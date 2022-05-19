import pandas as pd
from log import i_messages
import os

class file(i_messages):
    def __init__(self, file_name=None, data=None, encoding=False, delimiter='¨', columns='NoHeader', log_level=2):
        i_messages.__init__(self, log_level = log_level)
        self.__file_name = self.get_file_name(file_name)
        self.__ext       = self.get_extension() 
        self.__encoding  = encoding
        self.__data      = data
        self.__delimiter = delimiter
        self.__columns   = columns

    def get_file_name(self, file_name):
        file_name = 'result.pickle' if file_name == 'None' else file_name
        return './data/'+file_name if '/' not in file_name else file_name 
    
    def get_extension(self):
        return self.__file_name.split('.')[-1]
         
    def delete_file(self):        
        if os.path.exists(self.__file_name):
            os.remove(self.__file_name)
    
    def create_file_with_header(self):
        self.__data.to_csv(self.__file_name, encoding=self.__encoding, sep = self.__delimiter, header=False, index=False)
        
    def row_count(self):
        f = open(self.__file_name)                  
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.read # loop optimization

        buf = read_f(buf_size)
        while buf:
            lines += buf.count('\n')
            buf = read_f(buf_size)

        return lines

    def read(self, batch_size = -1, row_size = -1):
        self.print_messages(1101)
        __skiprows=batch_size+1
        if self.__ext == 'csv':
            if batch_size == -1:
                self.__data = pd.read_csv(self.__file_name, encoding=self.__encoding, sep = self.__delimiter, dtype=str, engine='python', keep_default_na=False)
            else:
                self.__data = pd.read_csv(self.__file_name, encoding=self.__encoding, sep = self.__delimiter, dtype=str, engine='python', keep_default_na=False , 
                                          skiprows=range(1,__skiprows), nrows=row_size)
        elif self.__ext in ('pickle',''):
            self.__data = pd.read_pickle(self.__file_name)
             
        self.print_messages(1001)
        yield self.__data
        
    
    def write(self):
        self.print_messages(1102) 
        if self.__ext == 'csv':  
            self.__data.to_csv(self.__file_name, mode='a', header=False, encoding=self.__encoding, sep = self.__delimiter, index=False, date_format='%Y-%m-%d %H:%M:%S')     
        else:
            if self.__ext != 'pickle':
                  self.__file_name = self.__file_name.split('.')[0] + '.pickle'
            self.__data.to_pickle(self.__file_name) 

        msg =  ' --> data added.'
        self.print_messages(1001, self.__file_name + msg)        
        
        