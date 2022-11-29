from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://192.168.0.1:7077").getOrCreate()
mode = ['taxis','locations']
print("spark session created")
if 'taxis' in mode:
    total = 0
    df = spark.read.parquet("/home/user/advdb/TaxiSource/yellow_tripdata_2022-01.parquet")
    total +=df.count()
    for i in range(1,7):
        print(f"reading month {i}")
        df_temp = spark.read.parquet(\
            f"/home/user/advdb/TaxiSource/yellow_tripdata_2022-0{i}.parquet")
        total +=df_temp.count()
        df=df.union(df_temp)
    assert df.count()==total
    print(f"total={total},count={df.count()}")
    df.write.parquet("/home/user/advdb/taxidata.parquet")
if 'locations' in mode:
    df = spark.read.csv("/home/user/advdb/TaxiSource/taxi+_zone_lookup.csv")
    print(df.schema)
    print(df.dtypes)
    df.write.parquet("/home/user/advdb/locdata.parquet")
print("end")