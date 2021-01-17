#test
import pyspark
import sys

from pyspark.sql import SparkSession

from pyspark.sql.types import TimestampType, StringType
import pyspark.sql.functions as F
import datetime


spark = SparkSession.builder.appName('World Health Data').getOrCreate()

#nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020", header=True, inferSchema=True)
table_name = sys.argv[1]
file_path = "gs://worldbank2021/rawdata/" + table_name
print("File name is " + file_path)
#"gs://worldbank2021/rawdata/health_nutrition_population"

df = spark.read.csv(file_path, header=True, inferSchema=True)


df.printSchema()
df.count()
df.describe().show()

df = df.dropDuplicates()

df.show()
print("There are {} rows in the Dataframe after dropping duplicates.".format(df.count()))

df = df.dropna(subset=['indicator_code'])
df.show()
print("There are {} rows in the Dataframe after dropping nulls.".format(df.count()))

bucket = "worldbank2021"
spark.conf.set('temporaryGcsBucket', bucket)

df.createOrReplaceTempView('wb_table')

# Perform filter by year on country = ARB.
#filter_rows = spark.sql(
 #       "SELECT country_name, indicator_name, value, year FROM wb_table where country_code = 'ARB'")
#filter_rows.show()
#filter_rows.printSchema()

df = df.withColumn('dateYear',
                       F.to_date(F.col('year').cast(StringType()),'yyyy'))


#filter_rows_date.show()
#filter_rows_date.printSchema()

df.show()

bq_table_name = 'worldbankhealth.' + table_name

df.write.format('bigquery') \
    .option('table', table_name) \
    .option('partitionField','dateYear')\
    .option('clusteredFields','indicator_code')\
    .save()