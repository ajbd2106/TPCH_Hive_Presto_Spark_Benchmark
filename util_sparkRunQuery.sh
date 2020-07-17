#!/bin/bash

INTERNAL_DATABASE=$1
INTERNAL_SETTINGSPATH=$2
INTERNAL_QUERYPATH=$3
INTERNAL_LOG_PATH=$4
INTERNAL_QID=$5
INTERNAL_CSV=$6

# Beeline command to execute
START_TIME="`date +%s`"
cd $SPARK_HOME
DRIVER_OPTIONS="--driver-memory 4g --driver-java-options -Dlog4j.configuration=file:///${output_dir}/log4j.properties"
EXECUTOR_OPTIONS="--executor-memory 2g --conf spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///${output_dir}/log4j.properties"
logInfo "Checking pre-reqs for running TPC-DS queries. May take a few seconds.."
bin/spark-sql --master yarn --deploy-mode client ${DRIVER_OPTIONS} ${EXECUTOR_OPTIONS} --conf spark.sql.crossJoin.enabled=true --conf spark.sql.catalogImplementation=hive -database $INTERNAL_DATABASE -f $INTERNAL_QUERYPATH   > $INTERNAL_LOG_PATH

RETURN_VAL=$?
END_TIME="`date +%s`"

if [[ $RETURN_VAL == 0 ]]; then
    status="SUCCESS"

    secs_elapsed="$(($END_TIME - $START_TIME))"
    echo $INTERNAL_QID, $secs_elapsed, $status >> $INTERNAL_CSV
else
    status="FAILURE"

    echo $INTERNAL_QID, " ", $status >> $INTERNAL_CSV
fi

# report status to terminal
echo "query$INTERNAL_QID: $status"
