""""
Spark ETL to extract top ten movies in each category per
decade
"""
import os
import sys
print(sys.path.insert(0, '/spark_etl'))

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as func
from pyspark.sql import functions as F
from etl_manager import EtlManager


from etl_conf import conf

def load_data(spark_session):
    product_df = spark_session.read.option("header", "true").csv('/spark_etl/data/products.csv')
    customer_df = spark_session.read.option("header", "true").csv('/spark_etl/data/customers.csv')
    cart_df = spark_session.read.option("header", "true").csv('/spark_etl/data/carts.csv')

    return product_df, customer_df, cart_df


def main(conf):
    spark_session = SparkSession.builder.config("spark.jars", "/spark_etl/conf/db/postgresql-42.6.0.jar").appName("CartDataTransform")\
        .getOrCreate()
        
    spark_session.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")


    product_df, customer_df, cart_df = load_data(spark_session)
    
    target_product_df, target_product_variation_df, target_customer_df, target_cart_df = execute_etl_process(product_df, customer_df, cart_df)
    
    save_process_data(target_product_df, target_product_variation_df, target_customer_df, target_cart_df)
    
def execute_etl_process( product_df, customer_df, cart_df):
    
    # Process product data into dataframe
    target_product_df = process_products(product_df, customer_df, cart_df)

    # Process product variations data into dataframe
    target_product_variation_df = process_product_variation(product_df, cart_df)

    # Process customer data into dataframe
    target_customer_df = process_customer(customer_df)
    
    # Process cart data into dataframe
    target_cart_df = process_cart(cart_df, target_product_variation_df)
    
    return target_product_df, target_product_variation_df, target_customer_df, target_cart_df
    
   
def save_process_data(target_product_df, target_product_variation_df, target_customer_df, target_cart_df):
    # Dump output of processing to database  
    # Prepare connection details for database
    jdbc_url = 'jdbc:postgresql://' + os.environ.get("DB_HOST") + ':' + os.environ.get("DB_PORT") + '/' + os.environ.get("DB_NAME")
    connection_properties = {
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    
    # write product data into database  
    table_name = "product"
    target_product_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

    # write product variation data into database  
    table_name = "product_variation"
    target_product_variation_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

    # write customer data into database  
    table_name = "customer"
    target_customer_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

    # write cart data into database  
    table_name = "cart"
    target_cart_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=connection_properties)

    # Dump output of processing to csv
    # Prepare path details for csv
    output_path = "/spark_etl/output"
    
    # write product data into csv  
    file_name = os.path.join(output_path, "target_product.csv")
    target_product_df.write.mode("overwrite").option("header", "true").csv(file_name)

    # write product variation data into csv  
    file_name = os.path.join(output_path, "target_product_variation.csv")
    target_product_variation_df.write.mode("overwrite").option("header", "true").csv(file_name)
    
    # write customer data into csv  
    file_name = os.path.join(output_path, "target_customer.csv")
    target_customer_df.write.mode("overwrite").option("header", "true").csv(file_name)
    
    # write cart data into csv  
    file_name = os.path.join(output_path, "target_cart.csv")
    target_cart_df.write.mode("overwrite").option("header", "true").csv(file_name)


def process_products(product_df, customer_df, cart_df):

    joined_df = product_df.join(cart_df, product_df["id"] == cart_df["product_id"], "inner").join(customer_df,
                                                                                                    cart_df[
                                                                                                        "customer_id"] == customer_df[
                                                                                                        "id"], "inner")

    target_product_df = joined_df.select(
        product_df["id"].alias("id"),
        customer_df["id"].alias("customer_id"),
        product_df["name"].alias("name"),
        F.split(product_df["sku"], "-").getItem(0).alias("sku"),
        F.unix_timestamp(cart_df["created_at"]).alias("create_time")
    )

    target_product_df.show()
    
    return target_product_df

def process_product_variation(product_df, cart_df):

    target_product_variation_df = product_df.join(
        cart_df,
        product_df["id"] == cart_df["product_id"],
        "inner"
    ).select(
        product_df["id"].alias("product_id"),
        cart_df["customer_id"],
        product_df["price"].cast("int").alias("price"),
        ((product_df["price"] % 1) * 100).cast("int").alias("decimal_part"),
        F.split(product_df["sku"], "-").getItem(1).alias("sku"),
        F.unix_timestamp(cart_df["created_at"], 'yyyy-MM-dd HH:mm:ss').cast("int").alias("create_time")
    )

    windowSpec = Window.orderBy(F.monotonically_increasing_id())
    target_product_variation_df = target_product_variation_df.withColumn("id", F.row_number().over(windowSpec)).select("id", "product_id", "customer_id", "price", "decimal_part", "sku", "create_time")
    target_product_variation_df.show()
   
    return target_product_variation_df

def process_customer(customer_df):

    target_customer_df = customer_df.withColumn('create_time', F.unix_timestamp(customer_df["created_at"]).alias("create_time")).select("id", "name", "create_time")

    target_customer_df.show()
    
    return target_customer_df

def process_cart(cart_df, product_variation_df):
    
    target_cart_df = cart_df.join(
        product_variation_df,
        "product_id",
        "inner"
    ).select(
        product_variation_df["id"].alias("product_variation_id"),
        cart_df["customer_id"],
        cart_df["quantity"],
        cart_df["created_at"].alias("create_time"),
        F.unix_timestamp(cart_df["delete_at"]).cast("timestamp").cast("bigint").alias("delete_time")
    )

    target_cart_df.show()
   
    return target_cart_df


if __name__ == '__main__':
    ENV = os.getenv('ENV', 'DEV')
    conf = conf[ENV]                      
    etl_obj = EtlManager(conf)
    etl_obj.check_create_database()
    main(conf)  