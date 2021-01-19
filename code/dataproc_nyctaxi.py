#test
import pyspark
import sys


from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('NYC Taxi').getOrCreate()

#nyctaxi_df = spark.read.parquet("gs://dataproc-nyc-taxi-2020/Week5/yellowtaxi1.parquet")
nyctaxi_df = spark.read.parquet("gs://dataproc-nyc-taxi-2020/Week5/yellowtaxi.parquet")

#nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020", header=True, inferSchema=True)
#nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020/yellow_tripdata_2019-01.csv", header=True, inferSchema=True)

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


#sc = pyspark.SparkContext()
#lines = sc.textFile(sys.argv[1])
#words = lines.flatMap(lambda line: line.split())
#wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
#output.saveAsTextFile("gs://dataproc-nyc-taxi-2020/output.txt")
