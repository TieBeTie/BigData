#!/usr/bin/env bash

IN_DIR="/data/minecraft-server-logs"
OUT_DIR="error_analysis_result"
NUM_REDUCERS=6

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="hobmapreducetask2_error_analysis" \
    -D stream.num.map.output.key.fields=1 \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options='-k1,1' \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input ${IN_DIR} \
    -output ${OUT_DIR} > /dev/null

hdfs dfs -cat ${OUT_DIR}/part-* | LC_ALL=C.UTF-8 LC_COLLATE=C sort -k2,2nr -k1,1 | head -10 