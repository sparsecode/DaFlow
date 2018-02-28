#!/bin/bash

JOB_NAME=$1
JOB_SUBTASK_NAME=$2
VENTURE=$3

STATUS=$4
FREQUENCY=$5
DATE=$6
HOUR=$7

V_PASSED_COUNT=$8
V_FAILED_COUNT=$9
EXECUTION_TIME=${10}

T_PASSED_COUNT=${11}
T_FAILED_COUNT=${12}
FAILURE_REASON="${13}"

echo "updating etl feed stat table for job=$JOB_NAME, job_subtask=$JOB_SUBTASK_NAME, venture=$VENTURE, date=$DATE"
QUERY="INSERT INTO TABLE etl_frwrk.etl_feed_stat PARTITION (job_name = '$JOB_NAME', venture = '$VENTURE') (job_subtask, status, frequency, data_date, data_hour, schema_validation_passed_data_count, schema_validation_failed_data_count, feed_execution_time, transformation_passed_data_count, transformation_failed_data_count, failure_reason) VALUES ('$JOB_SUBTASK_NAME', '$STATUS',  '$FREQUENCY',  '$DATE',  '$HOUR',  $V_PASSED_COUNT,  $V_FAILED_COUNT,  $EXECUTION_TIME, $T_PASSED_COUNT, $T_FAILED_COUNT, '$FAILURE_REASON');"
echo "Going to execute query: $QUERY"

hive -e "
SET mapred.job.queue.name=pipelines;
$QUERY
"

exit_code=$?
exit ${exit_code}