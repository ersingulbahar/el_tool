import argparse
from utils.module import install_modules
from connection.engine import engine
from log import i_messages
import gc
import time

if __name__ == "__main__":
    start_time = time.time()
    log=i_messages()
    
    parser = argparse.ArgumentParser(description='Python-Oracle Configuration')
    parser.add_argument('--query',             '-q',       metavar='N',    required=False, help='query to table')
    parser.add_argument('--environment',       '-e',       metavar='N',    required=False, help='environment: default:dds_dev, dds_test, dds_prod')
    parser.add_argument('--target_environment','-te',      metavar='N',    required=False, help='target environment: default:dds_dev, dds_test, dds_prod')
    parser.add_argument('--statement',         '-s',       metavar='N',    required=False, help='conneciton purpose: default:read, write, execute')
    parser.add_argument('--file_name',         '-f',       metavar='path', required=False, help='file path')
    parser.add_argument('--table_name',        '-tn',      metavar='N',    required=False, help='target table name to write')
    parser.add_argument('--mode',              '-m',       metavar='N',    required=False, help='mode : default:append for write, overwrite for read')
    parser.add_argument('--topic',             '-t',       metavar='N',    required=False, help='kafka topic : default:None')
    parser.add_argument('--encoding',          '-en',      metavar='N',    required=False, help='file encoding : default:utf-8')
    parser.add_argument('--delimiter',         '-d',       metavar='N',    required=False, help='file delimeter : default:¨')
    parser.add_argument('--batch',             '-b',       metavar='N',    required=False, help='batch size (int): default:-1')
    parser.add_argument('--enable_parallel',   '-ep',      metavar='N',    required=False, help='enable parallel (bool): default:False')
    parser.add_argument('--process_count',     '-pc',      metavar='N',    required=False, help='process count (bool): default:False')
    parser.add_argument('--thread_count',      '-tc',      metavar='N',    required=False, help='thread count (bool): default:False')
    parser.add_argument('--unique_column',      '-uc',     metavar='N',    required=False, help='unique column (string)')
    parser.add_argument('--log_level',         '-log',     metavar='N',    required=False, help='log level (int): default:2 (all logs), 1 (important logs)')

    args = parser.parse_args()
    
    log.print_messages(1000,'************************EL************************')
    engine(args)
    gc.collect()
    log.print_messages(1001,'************************EL************************\nTotal run time:'+str(time.time() - start_time)+' seconds.')
    