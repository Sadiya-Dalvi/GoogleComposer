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
    'start_date': datetime.datetime(2021, 1, 5),
}

with models.DAG(
        'composer_datapipeline',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:

    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
       task_id='create_dataproc_cluster',
       project_id='dataproc-300110',
       cluster_name='cluster-58-wb',
       num_workers=2,
       region='us-east1',
       master_machine_type='n1-standard-2',
       worker_machine_type='n1-standard-2'
    )

    etl_process = bash_operator.BashOperator(
        task_id='etl_process',
        # Run python file for ETL process.
        bash_command='python /home/airflow/gcs/data/etl_save.py')

#    dataproc_etl = dataproc_operator.DataProcPySparkOperator(
#        task_id = 'ETL_job',
#        #call the py file for processing
#        main='gs://dataproc-nyc-taxi-2020/code_deploy/etl_save.py',
#        cluster_name='cluster-58d6',
#        region='us-east1',
#        dataproc_pyspark_jars=[ ]
#    )

    dataproc_pyspark = dataproc_operator.DataProcPySparkOperator(
        task_id = 'Query_Data_spark_job',
        #call the py file for processing
        main='gs://worldbank2021/code/dataproc_nyctaxi.py',
        #main_jar='gs://dataproc-nyc-taxi-2020/code_deploy/myPy.jar',
        cluster_name='cluster-58d6',
        region='us-east1',
        dataproc_pyspark_jars=[ ]
    )

    delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        project_id='dataproc-300110',
        task_id = 'delete_dataproc_cluster',
        cluster_name='cluster-58d6',
        region='us-east1',
        trigger_rule = trigger_rule.TriggerRule.ALL_DONE
    )

    #create_dataproc_cluster >> dataproc_etl >> dataproc_pyspark >> delete_dataproc_cluster
    create_dataproc_cluster >> etl_process >> dataproc_pyspark >> delete_dataproc_cluster