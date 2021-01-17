#test
import pyspark
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('NYC Taxi').getOrCreate()

#nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020", header=True, inferSchema=True)
df = spark.read.parquet("gs://dataproc-nyc-taxi-2020/Week5/yellowtaxi1.parquet")

#print (df.dtypes)
#print (df.describe)

df.printSchema()

df.groupby('country').agg({'salary': 'mean'}).show()

df.groupby('country').count().show()

df.createOrReplaceTempView('user2')

df2 = spark.sql("SELECT country, last_name, first_name, salary \
                   FROM user2 \
                   WHERE salary > 1000")

df2.show()