#!/bin/bash
hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.0.jar \
    -input /user/hadoop/historical_stock_prices_pulito.csv \
    -output /user/hadoop/output \
    -mapper mapper.py \
    -reducer reducer.py \
    -file mapper.py \
    -file reducer.py \
    -file ../historical_stock_prices_pulito.csv