#!/usr/bin/env python
# coding: utf-8

# NYCbuswatcher Shipment --> Data Lake Migration

# usage:
# python shipments_2_parquet.py path_to_shipment_folder path_to_parquet_folder


# Step 1. Convert each JSON in "shipment_folder" to a single parquet file. 


import os, json
import pandas as pd
import numpy as np

import sys

# shipment_folder="/Volumes/nice/bigdata/bus_depot/bus-data-migrations/nyc/data/in/nyc_shipments/2022/2/2"
path_to_shipment_folder=sys.argv[1]

# get list of shipment filenames

shipment_list = []
for root, _, filenames in os.walk(path_to_shipment_folder):   
    for filename in filenames:
        shipment_list.append(os.path.join(root, filename))

# iterate over file list and write each JSON as a single parquet file with pyarrow

path_to_parquet_folder="/Volumes/nice/bigdata/bus_depot/bus-data-migrations/nyc/data/out/nyc_shipments_as_parquets/"
os.makedirs(out_path, exist_ok=False)

from pyspark.sql.types import IntegerType

# ensure every file has the same schema
def df_clean(df): 
    df["passenger_count"] = pd.to_numeric(df["passenger_count"],errors='coerce')
    if df.dtypes['passenger_count'] != "float64":
        df['passenger_count'] = df['passenger_count'].astype(dtype = 'float64')
    if df.dtypes['bearing'] != "float64":
        df['bearing'] = df['bearing'].astype(dtype = 'float64')
    if df.dtypes['next_stop_d_along_route'] != "float64":
        df['next_stop_d_along_route'] = df['next_stop_d_along_route'].astype(dtype = 'float64')
    if df.dtypes['next_stop_d'] != "float64":
        df['next_stop_d'] = df['next_stop_d'].astype(dtype = 'float64')   
    return df


for shipment in shipment_list:
    with open(shipment) as data_file:    
        data = json.load(data_file)
        try:
            df = pd.json_normalize(data, 'buses').convert_dtypes()
        except:
            pass
        df = df_clean(df)
        out_file=path_to_parquet_folder+shipment.split('/')[-1].split('.')[0]+".parquet"
        df.to_parquet(path_to_parquet_folder, engine="fastparquet")


# Step 2. Read all the parquets and repartition by route-hour using Pyspark

import os, json
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, LongType

spark = SparkSession.builder   \
    .master("local")   \
    .appName("nyc_reparition_individual_parquets_by_route_hour_1")   \
    .config("spark.executor.cores", 3)   \
    .getOrCreate()


spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Load all the parquets
df = spark.read.parquet(path_to_shipment_folder)


# Fix timestamp type
# https://sparkbyexamples.com/spark/pyspark-to_timestamp-convert-string-to-timestamp-type/
from pyspark.sql.functions import *

#Timestamp String to DateType
new_df = df.withColumn("timestamp",to_timestamp("timestamp"))
new_df.printSchema()


out_path=f"{path_to_parquet_folder}/partioned_by_route_hour"
os.makedirs(out_path, exist_ok=False)

# Save, adding date/hour columns on the fly for partitioning
# https://stackoverflow.com/questions/52527888/spark-partition-data-writing-by-timestamp/52528333#52528333
    
new_df     \
    .withColumn("year", year(col("timestamp")))     \
    .withColumn("month", month(col("timestamp")))    \
    .withColumn("day", dayofmonth(col("timestamp")))     \
    .withColumn("hour", hour(col("timestamp")))     \
    .repartition("year", "month", "day", "hour")     \
    .write     \
    .mode('overwrite')    \
    .partitionBy("year", "month", "day", "hour","route_short")     \
    .parquet(out_path)
