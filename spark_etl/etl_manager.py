#!usr/bin/env python
"""
Entry point for setting up database,
pull data file from remote location
"""
import os
import sys
import zipfile

import logging
sys.path.insert(0, os.getcwd())
from etl_conf import conf
from data_manager.file_handler import FileHandler
from data_manager.database_setup import SetupDb

# init logger
file_handler = logging.FileHandler(filename='logs/etl.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(filename)s:%(lineno)d] '
                           '%(levelname)s - %(message)s',
                    handlers=handlers)

logging.getLogger(__name__)

class EtlManager(object):
    def __init__(self, configs):
        self.configs = configs
        self.file_handler = FileHandler(self.configs)
        self.data_base = SetupDb(self.configs)


    def check_create_database(self):
        logging.info("Initialised database check/ creation")
        self.data_base.create_db_tables()


if __name__ == '__main__':
    ENV = os.getenv('ENV', 'DEV')
    conf = conf[ENV]                                                    # load dev / prod conf based on environment variable
    etl_obj = EtlManager(conf)
    etl_obj.check_create_database()
