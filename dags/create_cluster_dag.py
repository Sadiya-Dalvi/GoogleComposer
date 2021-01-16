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
        'create_cluster',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
       task_id='create_dataproc_cluster',
       project_id='dataproc-300110',
       cluster_name='cluster-58-wb',
       num_workers=2,
       region='us-east1',
       init_actions_uris=['gs://worldbank2021/code/connectors.sh'],
       master_machine_type='n1-standard-2',
       worker_machine_type='n1-standard-2'
    )


    #create_dataproc_cluster >> dataproc_etl >> dataproc_pyspark >> delete_dataproc_cluster
    create_dataproc_cluster