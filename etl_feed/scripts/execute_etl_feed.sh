#!/bin/bash
BLANK=""

VENTURE=$1
XML_FILE_PATH=$2
DATE=$3
SECOND_DATE=$4
STAT_SCRIPT_PATH=/mnt/hadoop/data-science.lazada.sg/gitlab-artifacts/etl_framework/scripts/etl_feed_stat.sh


 echo "Running etl feed with date"
    /opt/mapr/spark/spark-1.5.2/bin/spark-submit \
      --master yarn-client \
      --queue pipelines \
      --num-executors 4 \
      --executor-memory 4g \
      --class com.lzd.etlFramework.etl.feed.LaunchETLExecution \
      /mnt/hadoop/data-science.lazada.sg/gitlab-artifacts/etl_framework/etl_feed-assembly-0.1.0.jar \
      --venture $VENTURE \
      --xml_file_path $XML_FILE_PATH \
      --stat_script_file_path $STAT_SCRIPT_PATH \
      --date "${DATE:-$BLANK}" \
      --second_date "${SECOND_DATE:-$BLANK}"
