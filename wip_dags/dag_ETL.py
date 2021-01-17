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
    'start_date': datetime.datetime(2021, 1, 3),
}

with models.DAG(
        'ETL_test',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:


    dataproc_etl = dataproc_operator.DataProcPySparkOperator(
        task_id = 'ETL_job',
        #call the py file for processing
        main='gs://dataproc-nyc-taxi-2020/code_deploy/etl.py',
        cluster_name='cluster-58d6',
        region='us-east1',
        dataproc_pyspark_jars=[ ]
    )

    dataproc_etl