from kafka import KafkaConsumer, TopicPartition, KafkaProducer
from kafka.errors import KafkaError
import pandas as pd
import json
import ast
from log import i_messages
from connectors.file import file


class connection(i_messages):
    def __init__(self, params, topic, file_name, encoding, delimiter):
        i_messages.__init__(self) 
        self.__params    = params 
        self.__topic     = topic
        self.__file_name = file_name
        self.__encoding  = encoding
        self.__delimiter = delimiter

    def read(self, query='',  mode='overwrite'):
        self.print_messages(1201)
        consumer = KafkaConsumer(self.__topic,**self.__params)
        df_list=[]
        message_value=''
        logs=[]
        
        for message in consumer:
            try:
                message_value = message.value.decode(self.__encoding)
                message_value = ast.literal_eval(message_value)
                message_value = pd.DataFrame([message_value], columns = message_value.keys())
                df_list.append(message_value)
            except BaseException as e:
                logs.append([e,message_value[:10]])

        consumer.close(autocommit=True)
        try:
            self.__data = pd.concat(df_list, axis=0, ignore_index=True)
        except:
            self.__data = message_value if not len(message_value)<1 else pd.DataFrame(columns=['There', 'Is', 'No','Record'])
        
        self.print_messages(1001)
          
        if len(logs)>0:
            self.print_messages(1000,str(logs))
        
        file(file_name=self.__file_name, data=self.__data, mode=mode, encoding=self.__encoding, delimiter=self.__delimiter).write()

            
    def write(self, table_name=''):
        
        self.__data = file(file_name=self.__file_name, encoding=self.__encoding, delimiter=self.__delimiter).read()  

        data_list = self.__data.to_dict('records')
        
        self.print_messages(1211)
        producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode(self.__encoding), **self.__params)

        for __data in data_list:
            producer.send(self.__topic, __data)

        producer.flush()
        producer = KafkaProducer(retries=5)
        self.print_messages(1001)