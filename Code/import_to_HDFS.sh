#!/bin/sh
~/hadoop-2.7.7/bin/hdfs dfs -rm /*.csv
~/hadoop-2.7.7/bin/hdfs dfs -rm /*.parquet
~/hadoop-2.7.7/bin/hdfs dfs -rm -r /parsedData
~/hadoop-2.7.7/bin/hdfs dfs -put ~/advdb/TaxiSource/taxi+_zone_lookup.csv /taxizone.csv
~/hadoop-2.7.7/bin/hdfs dfs -put ~/advdb/TaxiSource/yellow_tripdata_2022-01.parquet /01.parquet
~/hadoop-2.7.7/bin/hdfs dfs -put ~/advdb/TaxiSource/yellow_tripdata_2022-02.parquet /02.parquet
~/hadoop-2.7.7/bin/hdfs dfs -put ~/advdb/TaxiSource/yellow_tripdata_2022-03.parquet /03.parquet
~/hadoop-2.7.7/bin/hdfs dfs -put ~/advdb/TaxiSource/yellow_tripdata_2022-04.parquet /04.parquet
~/hadoop-2.7.7/bin/hdfs dfs -put ~/advdb/TaxiSource/yellow_tripdata_2022-05.parquet /05.parquet
~/hadoop-2.7.7/bin/hdfs dfs -put ~/advdb/TaxiSource/yellow_tripdata_2022-06.parquet /06.parquet
~/hadoop-2.7.7/bin/hdfs dfs -ls /
