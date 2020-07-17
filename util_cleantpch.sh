#!/bin/bash

if [[ "$1" =~ ^[0-9]+$ && "$1" -gt "1" ]]; then
    if [[ "$2" == "orc" ]]; then
        DATABASE="tpch_orc_"$SCALE
    elif [[ "$2" == "parquet" ]]; then
        DATABASE="tpch_parquet_"$SCALE
    else
        echo "Invalid file format"
        exit 1
    fi

    echo "File format ok"
    # Command
    beeline -n ashujha -u jdbc:hive2://localhost:10000/tpch_parquet_10 -f droptable_tpch.hql
else
    echo "Invalid entry. Scale must also be greater than 1."
fi
