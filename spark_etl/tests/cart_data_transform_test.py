import pytest
import sys
import os
#print(sys.path.insert(0, '/spark_etl/'))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname('/spark_etl/jobs/'), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname('/spark_etl/data_manager/'), '..')))
from pyspark.sql import SparkSession
from spark_etl.etl_manager import EtlManager
from spark_etl.data_manager.database_setup import SetupDb
import psycopg2
from spark_etl.etl_conf import conf
import subprocess
from spark_etl.jobs.cart_data_transform import process_products, process_cart, process_customer, process_product_variation, load_data, execute_etl_process


spark_session = SparkSession.builder.appName("pytest").getOrCreate()

@pytest.fixture(scope="module")
def data():
    products_data = [
        (1000, 'Impact Wrench Ventilated', 9970.35, 'PHkXdiGyFyw-553', '2022-03-18'),
        (1001, 'Caliper Automatic', 781.45, 'BtFensRPpxC-982', '2023-02-25'),
        # Add more data
    ]
    products_schema = ["id", "name", "price", "sku", "created_at"]
    product_df = spark_session.createDataFrame(products_data, schema=products_schema)

    customers_data = [
        (9348, 'Juncken', '2023-07-27 11:05:03'),
        (9219, 'Reinhardt Römer AG & Co. KG', '2020-06-16 13:41:06'),
        # Add more data
    ]
    customers_schema = ["id", "name", "created_at"]
    customer_df = spark_session.createDataFrame(customers_data, schema=customers_schema)

    carts_data = [
        (8466, 1017, 7593, 8, '2023-01-06 12:16:51', '2020-09-06 15:44:01'),
        (1616, 1039, 519, 8, '2020-02-23 14:57:44', "2021-04-03 06:00:33"),
        # Add more data
    ]
    carts_schema = ["id", "product_id", "customer_id", "quantity", "created_at", "delete_at"]
    cart_df = spark_session.createDataFrame(carts_data, schema=carts_schema)
    
    product_variation_data = [
        (1, 1017, 7593, 200, 62, '553', 1698339092),
        (2, 1039, 519, 78, 45, '982', 1698329731),
        # Add more data
    ]
    product_variation_schema = ["id", "product_id", "customer_id", "price", "decimal_part", "sku", "create_time"]
    product_variation_df = spark_session.createDataFrame(product_variation_data, schema=product_variation_schema)

    return product_df, customer_df, cart_df, product_variation_df

def test_process_products(data):
    product_df, customer_df, cart_df, _ = data
    result_df = process_products(product_df, customer_df, cart_df)
    assert result_df is not None

def test_process_product_variation(data):
    product_df, customer_df, cart_df, _ = data
    result_df = process_product_variation(product_df, cart_df)
    assert result_df is not None

def test_process_customer(data):
    product_df, customer_df, cart_df, _ = data
    result_df = process_customer(customer_df)
    assert result_df is not None
    
def test_process_cart(data):
    product_df, customer_df, cart_df, product_variation_df = data

    result_df = process_cart(cart_df, product_variation_df)
    assert result_df is not None
    
def test_load_data():
    spark_session = SparkSession.builder.config("spark.jars", "/spark_etl/conf/db/postgresql-42.6.0.jar").appName("CartDataTransformTest").getOrCreate() 

    product_df, customer_df, cart_df = load_data(spark_session)
    
    assert product_df is not None
    assert customer_df is not None
    assert cart_df is not None
    
def test_execute_etl_process(data):
    ENV = os.getenv('ENVIRONMENT', 'DEV')
    configs = conf[ENV] 
    etl_obj = EtlManager(configs) 
    etl_obj.check_create_database() 
    setup_inst = SetupDb(configs)
    
    spark_session = SparkSession.builder.config("spark.jars", "/spark_etl/conf/db/postgresql-42.6.0.jar").appName("CartDataTransform")\
        .getOrCreate()
        
    spark_session.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")
    
        
    product_df, customer_df, cart_df, product_variation_df= data   
    target_product_df, target_product_variation_df, target_customer_df, target_cart_df = execute_etl_process(product_df, customer_df, cart_df)
    
    assert target_product_df.count() == 0
    assert target_customer_df.count() == 2
    assert target_cart_df.count() == 0
    assert target_product_variation_df.count() == 0


    

    
    