#!/usr/bin/env bash
# set up root directory
DAFLOW_ROOT=`dirname $PWD`

# shut down cluster
DAFLOW_WS=${DAFLOW_ROOT} docker-compose -f compose/docker-compose-daflow.yml down

# remove houst mount directory
rm -rf /tmp/daflow_hadoop_datanode
rm -rf /tmp/daflow_hadoop_namenode
