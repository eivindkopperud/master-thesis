#!/bin/bash
set -e

driver=spark-theodorc-eivinkop-d

network=spark-network
version=8

log(){
    echo -n $1" " && date
}

log "Not deleting previous containers"
#docker container rm -f $driver

    #-p 127.0.0.1:8080:8080 \
    #-p 127.0.0.1:7077:7077 \
    #amazoncorretto:$version \
    #--user 907:907 \

log "Starting Driver"
docker run \
    -it \
    --name $driver \
    --network $network \
    -h spark-driver \
    -v "$(pwd)/jars:/jars" \
    -v "$(pwd)/data-generation:/source-code" \
    hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 \
    bash
