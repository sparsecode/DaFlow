#!/usr/bin/env bash
# set up root directory
DAFLOW_ROOT=`dirname $PWD`
# shut down cluster
DAFLOW_ROOT=${DAFLOW_ROOT} docker-compose -f compose/docker-compose_daflow.yml down

# remove houst mount directory
rm -rf /tmp/hadoop_data
rm -rf /tmp/hadoop_name
