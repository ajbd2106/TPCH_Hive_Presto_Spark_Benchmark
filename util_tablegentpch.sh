#!/bin/bash

function timedate() {
    TZ="America/Los_Angeles" date
}

if [[ "$#" -ne 2 ]]; then
    echo "Incorrect number of arguments."
    echo "Usage is as follows:"
    echo "sh util_tablgentpch.sh SCALE FORMAT"
    exit 1
fi

if [[ "$1" =~ ^[0-9]+$ && "$1" -gt "1" ]]; then
    if [[ "$2" == "orc" || "$2" == "parquet" ]]; then
        echo "File format ok"
    else
        echo "Invalid. Supported formats are:"
        echo "orc"
        echo "parquet"
        exit 1
    fi

    # scale ~GB
    INPUT_SCALE="$1"
    # Name of clock file
    CLOCK_FILE="aaa_clocktime.txt"
    # Clock file
    rm $CLOCK_FILE
    echo "Old clock removed"
    echo "Created new clock"
    echo "Table gen time for TPC-H $INPUT_SCALE" > $CLOCK_FILE
    timedate >> $CLOCK_FILE
    echo "" >> $CLOCK_FILE

    # data generation
    # echo "Start data generation" >> $CLOCK_FILE
    # timedate >> $CLOCK_FILE
    # hdfs dfs -copyFromLocal tpch_resources /tmp
    # beeline -n ashujha -u jdbc:hive2://localhost:10000 -i settingsData.hql -f TPCHDataGen.hql --hiveconf SCALE=$INPUT_SCALE --hiveconf PARTS=$INPUT_SCALE --hiveconf LOCATION=/HiveTPCH_$INPUT_SCALE/ --hiveconf TPCHBIN=`grep -A 1 "fs.defaultFS" /usr/local/Cellar/hadoop/3.1.2/etc/hadoop/core-site.xml | tail -1 | sed -e 's/.*<value>\(.*\)<\/value>.*/\1/'`/tmp/tpch_resources > /dev/null
    # echo "End" >> $CLOCK_FILE
    # timedate >> $CLOCK_FILE
    # echo "" >> $CLOCK_FILE

    MAX_REDUCERS=2600 # ~7 years of data hortonworks
    REDUCERS=$((test ${INPUT_SCALE} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${INPUT_SCALE})

    # table creation
    hdfs dfs -mkdir -p /HiveTPCH_$INPUT_SCALE/
    hdfs dfs -chmod -R 777 /HiveTPCH_$INPUT_SCALE/
    echo "Start table generation" >> $CLOCK_FILE
    timedate >> $CLOCK_FILE
    beeline -n ashujha -u jdbc:hive2://localhost:10000 -i settings.hql -f tpch_ddl/createAllExternalTables.hql --hiveconf LOCATION=/HiveTPCH_$INPUT_SCALE/tpchdata/ --hiveconf DBNAME=tpch_$INPUT_SCALE --hiveconf REDUCERS=$REDUCERS
    echo "End" >> $CLOCK_FILE
    timedate >> $CLOCK_FILE
    echo "" >> $CLOCK_FILE

    if [[ "$2" == "orc" ]]; then
        # orc tables
        echo "Start orc table generation" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        beeline -n ashujha -u jdbc:hive2://localhost:10000 -i settings.hql -f tpch_ddl/createAllORCTables.hql --hiveconf ORCDBNAME=tpch_orc_$INPUT_SCALE --hiveconf SOURCE=tpch_$INPUT_SCALE --hiveconf REDUCERS=$REDUCERS
        echo "End" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        echo "" >> $CLOCK_FILE

        echo "Start orc analysis" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        beeline -n ashujha -u jdbc:hive2://localhost:10000 -i settings.hql -f tpch_ddl/analyze.hql --hiveconf DB=tpch_orc_$INPUT_SCALE --hiveconf REDUCERS=$REDUCERS
        echo "End" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        echo "" >> $CLOCK_FILE
    else
        # parquet tables
        echo "Start parquet table generation" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        beeline -n ashujha -u jdbc:hive2://localhost:10000 -i settings.hql -f tpch_ddl/createAllParquetTables.hql --hiveconf PARQUETDBNAME=tpch_parquet_$INPUT_SCALE --hiveconf SOURCE=tpch_$INPUT_SCALE
        echo "End" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        echo "" >> $CLOCK_FILE

        echo "Start parquet analysis" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        beeline -n ashujha -u jdbc:hive2://localhost:10000 -i settings.hql -f tpch_ddl/analyze.hql --hiveconf DB=tpch_parquet_$INPUT_SCALE
        echo "End" >> $CLOCK_FILE
        timedate >> $CLOCK_FILE
        echo "" >> $CLOCK_FILE
    fi

    echo "End time" >> $CLOCK_FILE
    timedate >> $CLOCK_FILE
else
    echo "Scale must be greater than 1."
fi
