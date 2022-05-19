from utils.module._module import module
from log import i_messages

class install_modules(i_messages):
    def __init__(self ): 
        i_messages.__init__(self)   
        try:
            __modules=module()
            __modules.modules=['argparse','pandas','kafka-python','sqlalchemy','sqlalchemy-hana', 'hdbcli','cx-oracle','pymysql','pymssql','configparser','psycopg2','elasticsearch','threaded','numpy','inspect-it']
            __modules.check()
            self.print_messages(1001)
        except:
            self.print_messages(5011)

install_modules()        