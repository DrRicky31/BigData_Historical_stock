#!/bin/bash
hdfs dfs -rm -r /user/hadoop3/output
hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.0.jar \
    -input /user/hadoop3/historical_stock_prices_pulito.csv \
    -output /user/hadoop3/output \
    -mapper mapper.py \
    -reducer reducer.py \
    -file mapper.py \
    -file reducer.py \
