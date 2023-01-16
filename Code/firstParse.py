from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.rdd import *
import os
import sys
import shutil
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

master = "spark://192.168.0.1:7077"


def createAll(mode):
    spark = SparkSession.builder.master(
        master).appName("Creating RDDs/DFs").getOrCreate()
    print("spark session created")
    print("modes: ", mode)
    if 'taxis' in mode:
        print("Parsing trip data...")
        total = 0
        df = spark.read.parquet("hdfs:///01.parquet")
        total += df.count()
        # combine data of all months
        for i in range(1, 7):
            print(f"reading month {i}")
            df_temp = spark.read.parquet(
                f"hdfs:///0{i}.parquet")
            total += df_temp.count()
            df = df.union(df_temp)
        # ensure that total tuples are equal to sum of all parts
        assert df.count() == total
        print(f"total:{total},count:{df.count()}")
        print(df.schema)
        print(df.dtypes)
        # Filter malformed data
        # drop rows with (any) null values
        df = df.dropna()
        # remove stray entries from other months and years
        df = df.filter(month(df.tpep_pickup_datetime) <= 6)
        df = df.filter(year(df.tpep_pickup_datetime) == 2022)
        # remove rows with negative values on certain columns
        df = df.filter((df.passenger_count >= 0) &
                       (df.trip_distance >= 0) & (df.fare_amount >= 0) &
                       (df.tip_amount >= 0) & (df.tolls_amount >= 0))

        print(f"count after cleaning:{df.count()}")
        if 'df' in mode:
            # output to parquet
            print("Outputting trip data DF as parquet...")
            df.write.parquet(
                "hdfs://192.168.0.1:9000/parsedData/taxidata.parquet")
        if 'rdd' in mode:
            # save RDD to HDFS
            print("Outputting trip data DF as RDD...")
            taxirdd = df.rdd
            taxirdd.saveAsPickleFile(
                "hdfs://192.168.0.1:9000/parsedData/taxidata")
    if 'locations' in mode:
        print("Parsing zone data...")
        df = spark.read.option("header", True).csv("hdfs:///taxizone.csv")
        print(df.schema)
        print(df.dtypes)
        # export DF as parquet
        if 'df' in mode:
            print("Outputting zone data DF as parquet...")
            df.write.parquet(
                "hdfs://192.168.0.1:9000/parsedData/zonedata.parquet")
        # save RDD to HDFS
        if 'rdd' in mode:
            print("Outputting zone data DF as RDD...")
            zonerdd = df.rdd
            zonerdd.saveAsPickleFile(
                "hdfs://192.168.0.1:9000/parsedData/zonedata")
    print("Done.")


if __name__ == "__main__":
    if (len(sys.argv) == 1 or 'all' in sys.argv):
        mode = ['taxis', 'locations', 'rdd', 'df']
        createAll(mode)
    else:
        createAll(sys.argv[1:])
