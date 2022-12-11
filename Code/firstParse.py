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
        print(f"total={total},count={df.count()}")
        # output to parquet
        if 'df' in mode:
            df.write.parquet("hdfs://192.168.0.1:9000/parsedData/taxidata.parquet")
        if 'rdd' in mode:
            # TODO save RDD to HDFS
            taxirdd = df.rdd
            taxirdd.saveAsPickleFile("hdfs://192.168.0.1:9000/parsedData/taxidata")
    if 'locations' in mode:
        df = spark.read.option("header", True).csv("hdfs:///taxizone.csv")
        print(df.schema)
        print(df.dtypes)
        # export DF as parquet
        if 'df' in mode:
            df.write.parquet("hdfs://192.168.0.1:9000/parsedData/zonedata.parquet")
        # TODO save RDD to HDFS
        if 'rdd' in mode:
            zonerdd = df.rdd
            zonerdd.saveAsPickleFile("hdfs://192.168.0.1:9000/parsedData/zonedata")
    print("end")


if __name__ == "__main__":
    if (len(sys.argv) == 1 or 'all' in sys.argv):
        mode = ['taxis', 'locations', 'rdd', 'df']
        createAll(mode)
    else:
        createAll(sys.argv[1:])
