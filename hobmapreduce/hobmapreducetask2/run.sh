#!/usr/bin/env bash

IN_DIR="/data/minecraft-server-logs"
OUT_DIR="error_analysis_result"
NUM_REDUCERS=6

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null 2>&1

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="hobmapreducetask2_error_analysis" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input ${IN_DIR} \
    -output ${OUT_DIR} > /dev/null 2>&1

hdfs dfs -cat ${OUT_DIR}/part-* | sort -k2,2nr -k1,1 | head -10 