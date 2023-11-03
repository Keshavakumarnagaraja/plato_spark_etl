#!/bin/bash

CONTAINER_NAME=$(docker ps --format '{{.Names}}' | grep "master")
docker exec -it "$CONTAINER_NAME" /bin/bash -c "cd /spark_etl && /usr/spark-3.5.0/bin/spark-submit --driver-class-path /spark_etl/conf/db/postgresql-42.6.0.jar /spark_etl/jobs/$1.py"
