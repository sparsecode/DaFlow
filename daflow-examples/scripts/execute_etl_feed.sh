#!/bin/bash


spark-submit \
--class com.abhioncbr.daflow.core.LaunchDaFlowSparkJobExecution \
daflow-examples/demo/artifacts/daflow-core-0.1-SNAPSHOT.jar \
-j example -c daflow-examples/demo/daflow-job-xml/json_etl_example.xml