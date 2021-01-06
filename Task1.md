# Dataset Used NYC City Taxi Data

Task 1
Create a python script which will run as a job in the data pipeline. The python script will read data from the nosql db and clean the data
remove null values from all columns, remove duplicate entries. The cleaned data is then written into parquet or orc file (alternatively can write to a json file)

Solution:
Nosql database used here is Datastore. The NYC TXI DATASET was already imported in datastore in the earlier assignments. This ws done using a python script. Wrote a python script(etl_save.py) to read NYC TAXI dataset and did transformations and converted it into parquet 
format. Once converted wrote it to the cloud storage.

The code is as below:

```
import sys
import csv
import datetime
from google.cloud import datastore
import pandas as pd
import os
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

df.drop_duplicates(subset = ['fare_amount','DOLocationID','PULocationID','tpep_pickup_datetime'], keep=False,inplace=True)

print("There are {} rows in the Dataframe after dropping duplicates.".format(df.count()))


os.system('rm -f yellowtaxi.parquet;')
#df.to_parquet("gs://dataproc-nyc-taxi-2020/Week5/yellowtaxi.parquet")

df.to_parquet("yellowtaxi.parquet")

#os.system('gsutil cp yellowtaxi.parquet us-central1 gs://us-central1-firstcomposer-12f2193c-bucket/data/;')
os.system('gsutil cp yellowtaxi.parquet us-east1 gs://dataproc-nyc-taxi-2020/Week5/;')
```

