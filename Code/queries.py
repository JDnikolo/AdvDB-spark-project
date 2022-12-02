from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from firstParse import master
import os
import sys
import time


def executeQ1(standalone=True):
    spark = SparkSession.builder.master(
        master).appName("Getting Q1").getOrCreate()
    start = time.time()
    df = spark.read.parquet("/home/user/advdb/parsedData/taxidata.parquet")
    zones = spark.read.parquet("/home/user/advdb/parsedData/zonedata.parquet")
    after_read = time.time()
    joined = df.join(zones,df.DOLocationID == zones.LocationID,"inner")
    result = joined.filter(
        (month(joined.tpep_pickup_datetime) == 3) & (joined.Zone == "Battery Park"))\
            .orderBy(joined.tip_amount, ascending=False).limit(1)
    #write to disk before time measurement
    result.write.option("header",True).mode("overwrite").csv("/home/user/advdb/parsedData/queryResults/Q1")
    end = time.time()
    q1result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q1result

def executeQ2(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q2").getOrCreate()
    start = time.time()
    df = spark.read.parquet("/home/user/advdb/parsedData/taxidata.parquet")
    after_read = time.time()
    df=df.withColumn("month",month(df.tpep_pickup_datetime))
    result = df.select(df.month,df.tolls_amount).filter(df.tolls_amount>0.0)
    result = result.groupBy("month").agg(sum(df.tolls_amount).alias("total_tolls"))
    result = result.orderBy(result.total_tolls,ascending=False)
    result.coalesce(1).write.option("header",True).mode("overwrite").csv("/home/user/advdb/parsedData/queryResults/Q2")
    end = time.time()
    q2result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q2result

def executeQ3API(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q3 using the DF/SQL API").getOrCreate()
    start = time.time()
    df = spark.read.parquet("/home/user/advdb/parsedData/taxidata.parquet")
    after_read = time.time()
    df1 = df.withColumn("fortnight",floor(dayofyear(df.tpep_pickup_datetime)/15 + 1)).filter(df.DOLocationID!=df.PULocationID)
    df1 = df1.select(df1.DOLocationID,df1.PULocationID,\
        df1.trip_distance,df1.total_amount,df1.fortnight)
    df1 = df1.groupBy(df1.fortnight).agg(\
        avg(df1.trip_distance).alias("average_distance"),
        avg(df1.total_amount).alias("average_cost"))
    result=df1.orderBy(df1.fortnight)
    # repartition shuffles the rows of a DF
    # but is better in terms of performance compared to coalesce()
    # since it can be done in parallel
    result.repartition(1).write.option("header",True).mode("overwrite").csv("/home/user/advdb/parsedData/queryResults/Q3API")
    end = time.time()
    q3result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q3result

def executeQ4(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q4").getOrCreate()
    start = time.time()
    df = spark.read.parquet("/home/user/advdb/parsedData/taxidata.parquet")
    after_read = time.time()
    df1 = df.withColumn("hour_of_day",hour(df.tpep_pickup_datetime))
    df1 = df1.groupBy(df1.hour_of_day).agg(\
        sum(df1.passenger_count).alias("total_passengers"),\
        max(df1.passenger_count).alias("max_passenger_ride"))
    df1 = df1.orderBy(df1.max_passenger_ride,ascending=False)
    max_passengers = df1.select("max_passenger_ride").collect()[0][0]
    result = df1.filter(df1.max_passenger_ride!=max_passengers)\
        .orderBy(df1.total_passengers,ascending=False).drop(df1.max_passenger_ride).limit(3)
    result.repartition(1).write.option("header",True).mode("overwrite").csv("/home/user/advdb/parsedData/queryResults/Q3API")
    end = time.time()
    q4result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q4result