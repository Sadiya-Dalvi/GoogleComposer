from __future__ import print_function

import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.operators import bigquery_operator
from airflow.utils.dates import days_ago

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.datetime(2020, 1, 4),
}

with models.DAG(
        'ETL_Save',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    run_python = bash_operator.BashOperator(
        task_id='run_ETL',
        # This example runs a Python script from the data folder to prevent
        # Airflow from attempting to parse the script as a DAG.
        bash_command='python /home/airflow/gcs/data/etl_save.py')