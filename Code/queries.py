import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from firstParse import master
import os
import sys
import time,datetime


def executeQ1(standalone=True):
    spark = SparkSession.builder.master(
        master).appName("Getting Q1").getOrCreate()
    start = time.time()
    df = spark.read.parquet("hdfs:///parsedData/taxidata.parquet")
    zones = spark.read.parquet("hdfs:///parsedData/zonedata.parquet")
    after_read = time.time()
    joined = df.join(zones,df.DOLocationID == zones.LocationID,"inner")
    result = joined.filter(
        (month(joined.tpep_pickup_datetime) == 3) & (joined.Zone == "Battery Park"))\
            .orderBy(joined.tip_amount, ascending=False).limit(1)
    #write to disk before time measurement
    result.write.option("header",True).mode("overwrite").csv("hdfs:///parsedData/queryResults/Q1")
    end = time.time()
    q1result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q1result

def executeQ2(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q2").getOrCreate()
    start = time.time()
    df = spark.read.parquet("hdfs:///parsedData/taxidata.parquet")
    after_read = time.time()
    df=df.withColumn("month",month(df.tpep_pickup_datetime))
    result = df.select(df.month,df.tolls_amount).filter(df.tolls_amount>0.0)
    result = result.groupBy("month").agg(sum(df.tolls_amount).alias("total_tolls"))
    result = result.orderBy(result.total_tolls,ascending=False)
    result.coalesce(1).write.option("header",True).mode("overwrite").csv("hdfs:///parsedData/queryResults/Q2")
    end = time.time()
    q2result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q2result

def executeQ3API(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q3 using the DF/SQL API").getOrCreate()
    start = time.time()
    df = spark.read.parquet("hdfs:///parsedData/taxidata.parquet")
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
    result.repartition(1).write.option("header",True).mode("overwrite").csv("hdfs:///parsedData/queryResults/Q3API")
    end = time.time()
    q3result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q3result

def executeQ3RDD(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q3 using RDDs").getOrCreate()
    start = time.time()
    df = spark.read.parquet("hdfs:///parsedData/taxidata.parquet")
    rdd = df.rdd
    after_read = time.time()
    def convToFortnight(row):
        dt = datetime.datetime(row.tpep_pickup_datetime)
        fortnight = dt.timetuple().tm_yday//15
        return (fortnight,(row.trip_distance,row.total_amount))
    rdd2 = rdd.filter(lambda x: x.DOLocationID!=x.PULocationID).collect()
    result= None
    end = time.time()
    q3result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q3result

def executeQ4(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q4").getOrCreate()
    start = time.time()
    df = spark.read.parquet("hdfs:///parsedData/taxidata.parquet")
    after_read = time.time()
    df1 = df.withColumn("hour_of_day",hour(df.tpep_pickup_datetime))
    df1 = df1.groupBy(df1.hour_of_day).agg(\
        sum(df1.passenger_count).alias("total_passengers"),\
        max(df1.passenger_count).alias("max_passenger_ride"))
    df1 = df1.orderBy(df1.max_passenger_ride,ascending=False)
    max_passengers = df1.select("max_passenger_ride").collect()[0][0]
    result = df1.filter(df1.max_passenger_ride!=max_passengers)\
        .orderBy(df1.total_passengers,ascending=False).drop(df1.max_passenger_ride).limit(3)
    result.repartition(1).write.option("header",True).mode("overwrite").csv("hdfs:///parsedData/queryResults/Q4")
    end = time.time()
    q4result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q4result

def executeQ5(standalone=True):
    spark = SparkSession.builder.master(master).appName("Getting Q5").getOrCreate()
    start = time.time()
    df = spark.read.parquet("hdfs:///parsedData/taxidata.parquet")
    after_read = time.time()
    df1 = df.select(df.tpep_pickup_datetime.alias("date"),df.tip_amount,df.fare_amount)
    df1 = df1.withColumn("tip_percent",df.fare_amount/df.tip_amount)\
        .withColumn("month",month(df1.date)).withColumn("day",dayofmonth(df1.date))
    df1 = df1.select(df1.day,df1.month,df1.tip_percent)
    m=1
    df2=df1.filter(df1.month==m)\
            .groupBy(df1.day,df1.month).agg(avg(df1.tip_percent).alias("avg_tip_percent")).limit(5)
    result = df2.orderBy(df2.avg_tip_percent,ascending=False)
    for m in [2,3,4,5,6]:
        df2=df1.filter(df1.month==m)\
            .groupBy(df1.day,df1.month).agg(avg(df1.tip_percent).alias("avg_tip_percent")).limit(5)
        result=result.union(df2.orderBy(df2.avg_tip_percent,ascending=False))
    result.repartition(1).write.option("header",True).mode("overwrite").csv("hdfs:///parsedData/queryResults/Q5")
    end = time.time()
    q5result = result
    if standalone:
        spark.stop()
    return (end-start),(end-after_read),q5result

def execAll():
    times= {}
    times["execTime"] = datetime.datetime.now()
    times["Q1total"],times["Q1postRead"],res = executeQ1()
    print(f"{datetime.datetime.now()}: Q1 done.")
    times["Q2total"],times["Q2postRead"],res = executeQ2()
    print(f"{datetime.datetime.now()}: Q2 done.")
    times["Q3APItotal"],times["Q3APIpostRead"],res = executeQ3API()
    print(f"{datetime.datetime.now()}: Q3API done.")
    times["Q4total"],times["Q4postRead"],res = executeQ4()
    print(f"{datetime.datetime.now()}: Q4 done.")
    times["Q5total"],times["Q5postRead"],res = executeQ5()
    print(f"{datetime.datetime.now()}: Q5 done.")
    return times

def exec1perhour():
    with open('log.txt','w') as sys.stdout:
        start = time.time()
        file = open('times.csv','w')
        columns = ['execTime', 'Q1total', 'Q1postRead',
        'Q2total', 'Q2postRead', 'Q3APItotal', 'Q3APIpostRead',
        'Q4total', 'Q4postRead', 'Q5total', 'Q5postRead']
        writer = csv.DictWriter(file,fieldnames=columns)
        writer.writeheader()
        while(time.time()-start <60*60*7):
            start=time.now()
            times = execAll()
            writer.writerow(times)  
            print(f"{datetime.datetime.now()}:All done, sleeping.")
            elapsed=time.now()-start
            time.sleep(30*60-elapsed)
