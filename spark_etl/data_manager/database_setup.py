#!usr/bin/env python

""""
Script for initial database setup
"""
import os
import sys
import logging
sys.path.insert(0, '/spark_etl')

# local imports
from data_manager.database_handler import PgDb
from etl_conf import conf

logging.getLogger(__name__)

class SetupDb(PgDb):
    def create_db_tables(self):
        
        product_table = """CREATE TABLE if not EXISTS product (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER,
            name VARCHAR(50),
            sku VARCHAR(50),
            create_time TIMESTAMP
        )PARTITION BY RANGE (create_time)
        """

        product_variation_table = """CREATE TABLE if not EXISTS product_variation (
            id SERIAL PRIMARY KEY,
            product_id INTEGER,
            customer_id INTEGER,
            price REAL,
            decimal_part INTEGER,
            sku VARCHAR(50),
            create_time TIMESTAMP
        )PARTITION BY RANGE (create_time)
        """

        customers_table = """CREATE TABLE if not EXISTS customer (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            create_time TIMESTAMP
        )PARTITION BY RANGE (create_time)
        """

        cart_table = """CREATE TABLE if not EXISTS cart (
            id SERIAL PRIMARY KEY,
            product_variation_id INTEGER,
            customer_id INTEGER,
            quantity INTEGER,
            create_time TIMESTAMP,
            delete_time TIMESTAMP
        )PARTITION BY RANGE (create_time)
        """

        product_index = "CREATE INDEX IF NOT EXISTS product_id_index ON product (id)"
        product_variation_index = "CREATE INDEX IF NOT EXISTS product_variation_id_index ON product_variation (id)"
        customer_index = "CREATE INDEX IF NOT EXISTS customer_id_index ON customer (id)"
        #cart_index = "CREATE INDEX IF NOT EXISTS cart_id_index ON cart (id)"
        
        self.write_query(product_table)
        self.write_query(product_index)
        logging.info("check/ create product done")

        self.write_query(product_variation_table)
        self.write_query(product_variation_index)
        logging.info("check/ create product_variation done")

        self.write_query(customers_table)
        self.write_query(customer_index)
        logging.info("check/ create customers done")

        self.write_query(cart_table)
        #self.write_query(cart_index)
        logging.info("check/ create cart done")
        
              

if __name__ == "__main__":
    ENV = os.getenv('ENVIRONMENT', 'DEV')
    configs = conf[ENV]
    setup_inst = SetupDb(configs)
    setup_inst.create_db_tables()
