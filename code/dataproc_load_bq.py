#test
#import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType
import pyspark.sql.functions as F
#from google.cloud import bigquery

#import datetime
#gs://worldbank2021/rawdata/wh_country_series_definition
#gs://worldbank2021/rawdata/wh_country_summary
#gs://worldbank2021/rawdata/wh_health_nutrition_population
#gs://worldbank2021/rawdata/wh_series_summary
#gs://worldbank2021/rawdata/wh_series_times


spark = SparkSession.builder.appName('World Health Data').getOrCreate()

#nyctaxi_df = spark.read.csv("gs://dataproc-nyc-taxi-2020", header=True, inferSchema=True)
table_name = sys.argv[1]
file_path = "gs://worldbank2021/rawdata/" + table_name
print("File name is " + file_path)
#"gs://worldbank2021/rawdata/health_nutrition_population"

df = spark.read.csv(file_path, header=True, inferSchema=True)


df.printSchema()
#df.count()
#df.describe().show()

df = df.dropDuplicates()

#df.show()

print("There are {} rows in the Dataframe after dropping duplicates.".format(df.count()))

bucket = "worldbank2021"
spark.conf.set('temporaryGcsBucket', bucket)

if table_name == 'wh_health_nutrition_population':
    df = df.dropna(subset=['indicator_code'])
    print("There are {} rows in the Dataframe after dropping nulls.".format(df.count()))

#df.show()



#df.createOrReplaceTempView('wb_table')

# Perform filter by year on country = ARB.
#filter_rows = spark.sql(
 #       "SELECT country_name, indicator_name, value, year FROM wb_table where country_code = 'ARB'")
#filter_rows.show()
#filter_rows.printSchema()

if table_name == 'wh_health_nutrition_population':
    df = df.withColumn('dateYear',
                       F.to_date(F.col('year').cast(StringType()),'yyyy'))

#filter_rows_date.show()
#filter_rows_date.printSchema()

#df.show()

# Construct a BigQuery client object.
#bq_client = bigquery.Client()

#table_id = 'dataproc-300110:worldbankhealth' + table_name

# If the table does not exist, delete_table raises
# google.api_core.exceptions.NotFound unless not_found_ok is True.
#bq_client.delete_table(table_id, not_found_ok=True)  # Make an API request.
#print("Deleted table '{}'.".format(table_id))

bq_table_name = 'worldbankhealth.' + table_name

if table_name == 'wh_health_nutrition_population':
    df.write.format('bigquery') \
        .option('table', bq_table_name) \
        .option('partitionField','dateYear') \
        .option('clusteredFields','indicator_code')\
        .save()
else:
    df.write.format('bigquery') \
        .option('table', bq_table_name) \
        .save()
