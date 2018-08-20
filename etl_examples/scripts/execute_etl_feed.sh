#!/bin/bash
BLANK=""

XML_FILE_PATH=$1
DATE=$2
SECOND_DATE=$3
STAT_SCRIPT_PATH={etl_feed_stat.sh path}


 echo "Running etl feed with date"
    /opt/mapr/spark/spark-<spark-version>/bin/spark-submit \
      --master yarn-client \
      --queue {queue-name} \
      --num-executors {} \
      --executor-memory {} \
      --class com.abhioncbr.etlFramework.etl_feed.LaunchETLExecution \
      {jar-path-initial-path}/etl_feed-assembly-{version}.jar \
      --xml_file_path $XML_FILE_PATH \
      --stat_script_file_path $STAT_SCRIPT_PATH \
      --date "${DATE:-$BLANK}" \
      --second_date "${SECOND_DATE:-$BLANK}"
