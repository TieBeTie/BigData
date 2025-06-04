#!/usr/bin/env bash

OUT_DIR="out"
NUM_REDUCERS=8

hadoop fs -rm -r -f -skipTrash ${OUT_DIR} ${OUT_DIR}.tmp >/dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="proper_names_count" \
    -D mapreduce.job.reduces=$NUM_REDUCERS \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input /data/wiki/en_articles \
    -output ${OUT_DIR}.tmp >/dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="proper_names_count_merge" \
    -D mapreduce.job.reduces=1 \
    -files reducer.py \
    -mapper cat \
    -reducer "python3 reducer.py" \
    -input ${OUT_DIR}.tmp \
    -output ${OUT_DIR} >/dev/null

hdfs dfs -cat ${OUT_DIR}/part-00000 | head -10 