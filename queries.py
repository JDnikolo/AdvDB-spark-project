from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from firstParse import master
import os
import sys
import time


def executeQ1():
    spark = SparkSession.builder.master(
        master).appName("Getting Q1").getOrCreate()
    df = spark.read.parquet("/home/user/advdb/parsedData/taxidata.parquet")
    result = df.filter(df.tpep_pickup_datetime == 3).orderBy(
        df.tip_amount, ascending=False).first()
    q1result = result.asDict()["tip_amount"]
