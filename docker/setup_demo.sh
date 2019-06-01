#!/usr/bin/env bash

# Create host mount directory and copy
mkdir -p /tmp/daflow_hadoop_namenode
mkdir -p /tmp/daflow_hadoop_datanode

DAFLOW_ROOT=`dirname $PWD`

# restart cluster
DAFLOW_WS=${DAFLOW_ROOT} docker-compose -f compose/docker-compose-daflow.yml down
DAFLOW_WS=${DAFLOW_ROOT} docker-compose -f compose/docker-compose-daflow.yml pull
rm -rf /tmp/daflow_hadoop_datanode/*
rm -rf /tmp/daflow_hadoop_namenode/*
sleep 5

DAFLOW_WS=${DAFLOW_ROOT} docker-compose -f compose/docker-compose-daflow.yml up -d
sleep 15

docker exec -it daflow-adhoc-1 /bin/bash /var/daflow/ws/docker/scripts/setup_demo_container.sh