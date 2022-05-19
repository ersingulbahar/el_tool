import json
from datetime import datetime
from inspect import getframeinfo, stack

class i_messages:
    def __init__(self,  log_level='None', path='./log/log_config/message.json'):
        self.messages = {}
        self.path = path
        self.read_messages()
        self.log_level=log_level
        
    def read_messages(self):    
        with open(self.path) as json_file:
            self.messages = json.load(json_file)
    
    def print_messages(self, m_id, ext_info=''):
        m_id = str(m_id)
        date = self.get_sysdate()
        msg=self.messages[m_id] 
        caller = getframeinfo(stack()[1][0])
        
        if type(ext_info)==int:
            ext_info=self.messages[str(ext_info)] 

        if ext_info !='':
            msg  = msg + " \n\t "+ str(ext_info)

        if m_id[0] in (["1","2","3"]):
            if str(self.log_level)=='1':
                pass
            if (str(self.log_level)=='2') or (str(self.log_level) == 'None'):
                print(date+"Info Code: "+m_id+" --> "+msg)        
        elif m_id[0]=="4":
            print(date+"Warning Code: "+m_id+"\n\n"+"MessageDetails ------------>  "+ \
                  msg+ "\n\nFileName ------------>  "+caller.filename+"\n\nLineNumber ------------>  "+str(caller.lineno)+"\n\n")
        elif m_id[0]=="5":
            print(date+"Error Code: "+m_id+"\n\n"+"MessageDetails ------------>  "+ \
                  msg+ "\n\nFileName ------------>  "+caller.filename+"\n\nLineNumber ------------>  "+str(caller.lineno)+"\n\n")
            exit()
            
    def get_sysdate(self):
        return datetime.today().strftime('%Y-%m-%d-%H:%M:%S: ')