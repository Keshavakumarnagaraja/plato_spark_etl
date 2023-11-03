#!usr/bin/env python

"""
Config manager for etl

"""

import logging
import os

logging.getLogger(__name__)

class Config(object):
    pass

class DevelopmentConfig(Config):
    DEBUG = True
    
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER")
    db_host = os.environ.get("DB_HOST")
    db_password = os.environ.get("DB_PASSWORD")
    db_port = os.environ.get("DB_PORT")

    download_folder = os.getcwd() + os.sep + "data"

    LOG_LEVEL = logging.WARN
    LOGS_ROOT = "/logs"
    
    pg_db = {"pg_data_lake": {"dbname": db_name,
                              "user": db_user,
                              "host": db_host,
                              "password": db_password,
                              "port": db_port

                                 }}
    d_set_1_url = "http://files.grouplens.org/datasets/movielens/ml-20m-youtube.zip"
    d_set_2_url = "http://files.grouplens.org/datasets/movielens/ml-10m.zip"



class ProductionConfig(Config):
    pass


class TestingConfig(Config):
    pass

conf = {
    'DEV': DevelopmentConfig,
    'PROD': ProductionConfig,
    'TEST': TestingConfig
}
