# Task 2 - Write another python script to read from the parquet or orc file and create a descriptive summary

Compute sum of a numeric column. 
You can take it a step further by grouping it on a specific. Calculated Trips by passenger count ordered by passenger count in descending order.


Solution:
Wrote a script (dataproc_nyctaxi.py) to read from the parquet file generated in Task 1 and ran it as a job on a Dataproc cluster.

```
import pyspark
import sys

from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('NYC Taxi').getOrCreate()

nyctaxi_df = spark.read.parquet("gs://dataproc-nyc-taxi-2020/Week5/yellowtaxi.parquet")

nyctaxi_df.describe().show()

# Find the average trip distance 
nyctaxi_df.agg({'trip_distance': 'mean'}).show()

# show first 10 rows
output = nyctaxi_df.show(10)

# Trips by passenger count ordered by passenger count in descending order

nyctaxi_df.groupby('passenger_count').count().orderBy(nyctaxi_df.passenger_count.desc()).show()


# Average trip distance by passenger count

nyctaxi_df.groupby('passenger_count').agg({'trip_distance': 'mean'}).show()


#  Number of trips by passenger count, sorted in descending 
# order of passenger count
nyctaxi_df.groupby('passenger_count').count(). \
orderBy(nyctaxi_df.passenger_count.desc()).show()
```
