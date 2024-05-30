#!/bin/bash
hdfs dfs -rm -r /user/hadoop3/SparkCore3_result
$SPARK_HOME/bin/spark-submit --master yarn /home/pc/Documents/PROGETTO/BigData_Historical_stock/3-SparkCore/SparkCore_job3.py

