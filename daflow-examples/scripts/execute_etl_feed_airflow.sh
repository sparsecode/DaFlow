#!/usr/bin/env bash
{{ params.conf['spark_home'] }}/spark-submit --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.hive.convertMetastoreParquet=false \
--conf spark.yarn.executor.memoryOverhead={{ params.conf['memoryOverhead'] }} \
--conf spark.memory.useLegacyMode=true \
--conf spark.shuffle.memoryFraction=0.5 \
--conf spark.storage.memoryFraction=0.5 \
--conf spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec \
--master yarn-client --queue {{ params.conf['queue'] }} \
--num-executors {{ params.conf['num_executors'] }} --driver-memory {{ params.conf['driver_memory'] }} --executor-cores {{ params.conf['executor_cores'] }} \
--executor-memory {{ params.conf['executor_memory'] }} \
--class {{ params.conf['entry_class'] }} \
{{params.conf['app'] }} \
--date "{{ execution_date }}" {{params.arg_string }}