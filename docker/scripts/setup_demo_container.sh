#!/usr/bin/env bash
hadoop fs -mkdir -p /user/root/daflow-examples/demo/sample-data
hadoop fs -copyFromLocal  -f /var/daflow/ws/daflow-examples/demo/sample-data/json_data.json /user/root/daflow-examples/demo/sample-data
hadoop fs -mkdir -p /user/root/daflow-examples/demo/sample-data/daflow-result
