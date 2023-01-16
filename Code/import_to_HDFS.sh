#!/bin/sh
#hadoop/bin must be part of PATH for this to work
#the TaxiSource folder should be in the parent directory 
hdfs dfs -rm /*.csv
hdfs dfs -rm /*.parquet
hdfs dfs -rm -r /parsedData
hdfs dfs -put ../TaxiSource/taxi+_zone_lookup.csv /taxizone.csv
hdfs dfs -put ../TaxiSource/yellow_tripdata_2022-01.parquet /01.parquet
hdfs dfs -put ../TaxiSource/yellow_tripdata_2022-02.parquet /02.parquet
hdfs dfs -put ../TaxiSource/yellow_tripdata_2022-03.parquet /03.parquet
hdfs dfs -put ../TaxiSource/yellow_tripdata_2022-04.parquet /04.parquet
hdfs dfs -put ../TaxiSource/yellow_tripdata_2022-05.parquet /05.parquet
hdfs dfs -put ../TaxiSource/yellow_tripdata_2022-06.parquet /06.parquet
hdfs dfs -ls /