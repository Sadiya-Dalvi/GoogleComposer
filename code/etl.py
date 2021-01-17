import sys
import csv
import datetime
from google.cloud import datastore
import pandas as pd
#from google.cloud import storage
#from pyspark.context import SparkContext
#from pyspark.sql.session import SparkSession


client = datastore.Client()

query = client.query(kind='YellowTripdata2019')


query_iter = query.fetch()

df = pd.DataFrame(query_iter)

print (df.isnull().sum().sort_index()/len(df))

print("There are {} rows in the Dataframe.".format(df.count()))
df = df.dropna(how='any',axis=0)


print("There are {} rows in the Dataframe after dropping Null.".format(df.count()))


#df = df[df.duplicated(['fare_amount','DOLocationID','PULocationID','tpep_pickup_datetime'])]

df.drop_duplicates(subset = ['fare_amount','DOLocationID','PULocationID','tpep_pickup_datetime'], keep=False,inplace=True)

print("There are {} rows in the Dataframe after dropping duplicates.".format(df.count()))


#storage_client = storage.Client()

#bucket = storage_client.bucket("gs://nyc-taxi-data-sadiya/")
#blob = bucket.blob("Week5/yellowtaxi.parquet")
#blob.delete()

#df.to_parquet("gs://dataproc-nyc-taxi-2020/Week5/yellowtaxi.parquet")
df.to_parquet("yellowtaxi.parquet")

# Get a reference to the Spark Session 
#sc = SparkContext()
#spark = SparkSession(sc)
# convert from Pandas to Spark 
#sparkDF = spark.createDataFrame(df)
# perform an operation on the DataFrame
#print(sparkDF.count())

# DataFrame head 
#sparkDF.show(n=3)

#for entity in query_iter:
#    print(entity)
