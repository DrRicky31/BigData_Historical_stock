#!/bin/bash
hadoop jar $HADOOP_HOME/streaming/hadoop-streaming-3.4.0.jar \
    -input /riccardo/hadoop/historical_stock_prices_pulito.csv \
    -output /riccardo/hadoop/output \
    -mapper /home/riccardo/Documents/BigData_Historical_stock/mapper.py \
    -reducer /home/riccardo/Documents/BigData_Historical_stock/reducer.py \
    -file /home/riccardo/Documents/BigData_Historical_stock/mapper.py \
    -file /home/riccardo/Documents/BigData_Historical_stock/reducer.py \
    -file /home/riccardo/Documents/BigData_Historical_stock/historical_stock_prices_pulito.csv
