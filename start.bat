@echo off

docker build -t plato/spark .
docker-compose up --build -d
