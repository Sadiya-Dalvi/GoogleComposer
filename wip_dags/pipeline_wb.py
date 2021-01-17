from __future__ import print_function

import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
# from airflow.contrib.operators import bigquery_operator
from airflow.utils.dates import days_ago

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.datetime(2021, 1, 13),
}

with models.DAG(
        'wb_datapipeline',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:




    dataproc_pyspark = dataproc_operator.DataProcPySparkOperator(
        task_id = 'Query_Data_spark_job',
        #call the py file for processing
    #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
        main='gs://worldbank2021/code/dataproc_wb.py',
        cluster_name='cluster-58-wb',
        region='us-east1',
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
    )


    #create_dataproc_cluster >> dataproc_etl >> dataproc_pyspark >> delete_dataproc_cluster
    #create_dataproc_cluster >> etl_process >> dataproc_pyspark >> delete_dataproc_cluster
    dataproc_pyspark
