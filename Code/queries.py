from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from firstParse import master
import time

def executeQ1(standalone=False):
    spark = SparkSession.builder.master(master)\
        .config("spark.executor.memory", "8g")\
        .appName("Getting Q1").getOrCreate()
    start = time.time()
    df = spark.read.parquet(
        "hdfs://192.168.0.1:9000/parsedData/taxidata.parquet")
    zones = spark.read.parquet(
        "hdfs://192.168.0.1:9000/parsedData/zonedata.parquet")
    after_read = time.time()
    joined = df.join(zones, df.DOLocationID == zones.LocationID, "inner")
    result = joined.filter(
        (month(joined.tpep_pickup_datetime) == 3) & (joined.Zone == "Battery Park"))\
        .orderBy(joined.tip_amount, ascending=False).limit(1)
    # write to disk before time measurement
    result.write.option("header", True).mode("overwrite").csv(
        "hdfs://192.168.0.1:9000/parsedData/queryResults/Q1")
    end = time.time()
    q1result = result
    if standalone:
        spark.stop()
    return (end-start), (end-after_read), q1result


def executeQ2(standalone=False):
    spark = SparkSession.builder.master(master)\
        .config("spark.executor.memory", "8g")\
        .appName("Getting Q2").getOrCreate()
    start = time.time()
    df = spark.read.parquet(
        "hdfs://192.168.0.1:9000/parsedData/taxidata.parquet")
    after_read = time.time()
    df = df.withColumn("month", month(df.tpep_pickup_datetime))
    result = df.select(df.month, df.tolls_amount).filter(df.tolls_amount > 0.0)
    result = result.groupBy("month").agg(
        sum(df.tolls_amount).alias("total_tolls"))
    result = result.orderBy(result.total_tolls, ascending=False)
    result.coalesce(1).write.option("header", True).mode("overwrite").csv(
        "hdfs://192.168.0.1:9000/parsedData/queryResults/Q2")
    end = time.time()
    q2result = result
    if standalone:
        spark.stop()
    return (end-start), (end-after_read), q2result


def executeQ3API(standalone=False):
    spark = SparkSession.builder.master(master)\
        .config("spark.executor.memory", "8g")\
        .appName("Getting Q3 using the DF/SQL API").getOrCreate()
    start = time.time()
    df = spark.read.parquet(
        "hdfs://192.168.0.1:9000/parsedData/taxidata.parquet")
    after_read = time.time()
    df1 = df.withColumn("fortnight", floor(dayofyear(
        df.tpep_pickup_datetime)/15 + 1)).filter(df.DOLocationID != df.PULocationID)
    df1 = df1.select(df1.DOLocationID, df1.PULocationID,
                     df1.trip_distance, df1.total_amount, df1.fortnight)
    df1 = df1.groupBy(df1.fortnight).agg(
        avg(df1.trip_distance).alias("average_distance"),
        avg(df1.total_amount).alias("average_cost"))
    result = df1.orderBy(df1.fortnight)
    # repartition shuffles the rows of a DF
    # but is better in terms of performance compared to coalesce()
    # since it can be done in parallel
    result.repartition(1).write.option("header", True).mode("overwrite").csv(
        "hdfs://192.168.0.1:9000/parsedData/queryResults/Q3API")
    end = time.time()
    q3result = result
    if standalone:
        spark.stop()
    return (end-start), (end-after_read), q3result


def executeQ3RDD(standalone=False):
    spark = SparkSession.builder.master(master)\
        .config("spark.executor.memory", "8g")\
        .appName("Getting Q3 using RDDs").getOrCreate()
    start = time.time()
    rdd = spark.sparkContext.pickleFile(
        "hdfs://192.168.0.1:9000/parsedData/taxidata")
    after_read = time.time()

    def convToFortnight(row):
        dt = row.tpep_pickup_datetime
        fortnight = dt.timetuple().tm_yday//15 + 1
        return (fortnight, (row.trip_distance, row.total_amount, 1))
    rdd2 = rdd.filter(lambda x: x.DOLocationID != x.PULocationID)
    rdd2 = rdd2.map(convToFortnight).reduceByKey(
        lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
    rdd2 = rdd2.mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))
    result = rdd2.collect()
    end = time.time()
    q3result = result
    if standalone:
        spark.stop()
    return (end-start), (end-after_read), q3result


def executeQ4(standalone=False):
    spark = SparkSession.builder.master(master)\
        .config("spark.executor.memory", "8g")\
        .appName("Getting Q4").getOrCreate()
    start = time.time()
    df = spark.read.parquet(
        "hdfs://192.168.0.1:9000/parsedData/taxidata.parquet")
    after_read = time.time()
    df1 = df.withColumn("day", dayofweek(df.tpep_pickup_datetime))
    df1 = df1.withColumn("hour_of_day", hour(df1.tpep_pickup_datetime))
    df1 = df1.groupBy(df1.hour_of_day, df1.day).agg(
        max(df1.passenger_count).alias("max_passenger_ride"))
    win = Window.partitionBy(df1.day).orderBy(df1.max_passenger_ride.desc())
    result = df1.withColumn("rank", rank().over(win))
    result = result.filter(result.rank < 4).orderBy([result.day, result.rank])
    result.repartition(1).write.option("header", True).mode("overwrite").csv(
        "hdfs://192.168.0.1:9000/parsedData/queryResults/Q4")
    end = time.time()
    q4result = result
    if standalone:
        spark.stop()
    return (end-start), (end-after_read), q4result


def executeQ5(standalone=False):
    spark = SparkSession.builder.master(master)\
        .config("spark.executor.memory", "8g")\
        .appName("Getting Q5").getOrCreate()
    start = time.time()
    df = spark.read.parquet(
        "hdfs://192.168.0.1:9000/parsedData/taxidata.parquet")
    after_read = time.time()
    df1 = df.select(df.tpep_pickup_datetime.alias(
        "date"), df.tip_amount, df.fare_amount)
    df1 = df1.withColumn("tip_percent",((df1.tip_amount/df1.fare_amount)*100.0))\
        .withColumn("month", month(df1.date)).withColumn("day", dayofmonth(df1.date))
    df1 = df1.select(df1.day, df1.month, df1.tip_percent)
    m = 1
    df2 = df1.filter(df1.month == m).groupBy(df1.day, df1.month)\
            .agg(max(df1.tip_percent).alias("max_tip_percent"))
    df2=df2.orderBy(df2.max_tip_percent, ascending=False).limit(5)
    result = df2.orderBy(df2.max_tip_percent, ascending=False)
    for m in [2, 3, 4, 5, 6]:
        df2 = df1.filter(df1.month == m).groupBy(df1.day, df1.month)\
                .agg(max(df1.tip_percent).alias("max_tip_percent"))
        df2=df2.orderBy(df2.max_tip_percent, ascending=False).limit(5)
        result = result.union(df2)
    result = result.select(result.month,result.day,result.max_tip_percent)
    result.repartition(1).write.option("header", True).mode("overwrite").csv(
        "hdfs://192.168.0.1:9000/parsedData/queryResults/Q5")
    end = time.time()
    q5result = result
    if standalone:
        spark.stop()
    return (end-start), (end-after_read), q5result
