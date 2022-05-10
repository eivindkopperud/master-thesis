#!/bin/bash
set -e

master=spark-theodorc-eivinkop-m
worker=spark-theodorc-eivinkop-w
network=spark-network
version=3.2.0-debian-10-r89 

log(){
    echo -n $1" " && date
}


log "Deleting previous containers"
docker container rm -f $master
docker container rm -f $worker


log "Deleting old network"
#docker network rm $network || ls

log "Creating network"
#docker network create --driver bridge $network

log "Starting Master"
docker run -d \
    -p 127.0.0.1:8080:8080 \
    -p 127.0.0.1:7077:7077 \
    --name $master \
    --network $network \
    -h spark \
    -e SPARK_MODE=master \
    -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
    -e SPARK_RPC_ENCRYPTION_ENABLED=no \
    -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
    -e SPARK_SSL_ENABLED=no \
    bitnami/spark:$version \

log "Starting Worker "
docker run -d \
    --name $worker \
    -p 127.0.0.1:8081:8081 \
    -h spark-worker \
    --network $network \
    --user 0:0 \
    -e SPARK_MODE=worker \
    -e SPARK_MASTER_URL=spark://spark:7077 \
    -e SPARK_RPC_AUTHENTICATION_ENABLED=no \
    -e SPARK_RPC_ENCRYPTION_ENABLED=no \
    -e SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no \
    -e SPARK_SSL_ENABLED=no \
    -v "$(pwd)/jars:/jars" \
    -v "$(pwd)/data-generation:/source-code" \
    bitnami/spark:$version \

