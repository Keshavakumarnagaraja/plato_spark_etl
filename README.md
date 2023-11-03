# spark-postgre-etl
Dockerised Apache spark 2.x job to Transform the data, process it and store the results into post-gres database


## Setting up the environments

Requirements:
- git 
- docker
- docker-compose

1. Download the code base from github repo :

  
2. Change directory to folder:
  
    **``cd spark-postgres-etl``**

3. Build and start the spark container using docker compose file from this folder using command:
  
    **``docker-compose up --build -d``**
  
4. Log into the master docker container using command:

    **``docker exec -it pyspark-postgres-plato-etl-master-1 /bin/bash``**
  
5. move to /spark_etl folder

    **``cd /spark_etl``**
  
6. install the python requirements using pip:
 
    **``pip install -r requirements.txt``**
  
7. run script to pull data to local, create database, insert default schemas, data
   **``python etl_manager.py``**

. Run the first spark job using spark-submit

    **``/usr/spark-2.4.1/bin/spark-submit --driver-class-path /spark_etl/conf/db/postgresql-42.6.0.jar  /spark_etl/jobs/cart_data_transform.py``**
  
  
Database schema
Four tables - cart,customer,product,product_variation

1. product: stores the transformed product data

 product 
	id  Primary key,
	customer_id,
	name,
	sku,
	create_time


2. product_variation: stores the transformed product data and its variations
product_variation
	id int4 NULL,
	product_id,
	customer_id,
	price,
	decimal_part,
	sku,
	create_time

3. product_variation: stores the transformed customer data 
customers 
	id,
	name,
	create_time

4. product_variation: stores the transformed cart data
cart 
	id ,
	product_variation_id,
	customer_id,
	quantity,
	create_time,
	delete_time
