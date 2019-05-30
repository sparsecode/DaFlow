#!/usr/bin/env bash
# Create host mount directory and copy
mkdir -p /tmp/hadoop_name
mkdir -p /tmp/hadoop_data

DAFLOW_ROOT=`dirname $PWD`
# restart cluster
DAFLOW_WS=${DAFLOW_ROOT} docker-compose -f compose/docker-compose_daflow.yml down
DAFLOW_WS=${DAFLOW_ROOT} docker-compose -f compose/docker-compose_daflow.yml pull
rm -rf /tmp/hadoop_data/*
rm -rf /tmp/hadoop_name/*
sleep 5
DAFLOW_WS=${DAFLOW_ROOT} docker-compose -f compose/docker-compose_daflow.yml up -d
sleep 15
