from kafka import KafkaProducer
import os, random


from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import mean, col, desc, count, isnull, isnan, when, rank, sum
import pandas as pd
import matplotlib.pyplot as plt


spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.ui.showConsoleProgress", "false") \
    .getOrCreate()
    # .config("spark.master", "spark://127.0.0.1:7077") \


spark.sparkContext.setLogLevel(logLevel='ERROR')


producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

csv_dir = 'uber_test_split'
uber = spark.read.load(csv_dir, format="csv", header="false", inferSchema="true")

rdd=uber.rdd.map(lambda x:
        (x[0] + "," + str(x[1]) + "," + str(x[2]) + "," + x[3],)
    )

# df=rdd.toDF()
# df.show()
# rdd.foreach(lambda x: producer.send('uber', value=x[0].encode('utf-8'))) # _pickle.PicklingError: Could not serialize object: TypeError: cannot pickle '_thread.RLock' object
for row in rdd.collect():
    producer.send('uber_a', value=row[0].encode('utf-8'))

print(f'Sent {rdd.count()} lines in total.')
