import pytest
import sys
import os
from unittest.mock import patch
from etl_conf import conf
import psycopg2

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname('/spark_etl/data_manager/'), '..')))
from spark_etl.data_manager.database_setup import SetupDb


def test_create_db_tables():
    ENV = os.getenv('ENVIRONMENT', 'DEV')
    configs = conf[ENV]
    setup_inst = SetupDb(configs)

    setup_inst.create_db_tables()

    try:
        with setup_inst.postgres_conn.cursor() as cur:
            cur.execute("SELECT * FROM product")
            cur.execute("SELECT * FROM product_variation")
            cur.execute("SELECT * FROM customer")
            cur.execute("SELECT * FROM cart")
    except psycopg2.Error as e:
        assert False, f"Error occurred during querying tables: {e}"