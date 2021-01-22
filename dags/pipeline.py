from __future__ import print_function

import datetime
import airflow
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule
# from airflow.contrib.operators import bigquery_operator
from airflow.utils.dates import days_ago
from datetime import timedelta

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with models.DAG(
        'Project_WH_Parallel_Datapipeline',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # import datetime
    # gs://worldbank2021/rawdata/wh_country_series_definition
    # gs://worldbank2021/rawdata/wh_country_summary
    # gs://worldbank2021/rawdata/wh_health_nutrition_population
    # gs://worldbank2021/rawdata/wh_series_summary
    # gs://worldbank2021/rawdata/wh_series_times

    # def greeting():
    #  import logging
    # logging.info('Hello DataPipeline for World Health Data!')

    # An instance of an operator is called a task. In this case, the
    # hello_python task calls the "greeting" Python function.
    # hello_python = python_operator.PythonOperator(
    #   task_id='KickOff',
    # python_callable=greeting)

    create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        project_id='dataproc-300110',
        cluster_name='cluster-58-wb',
        num_workers=2,
        region='us-east1',
        init_actions_uris=['gs://worldbank2021/code/init_cluster.sh'],
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2'
    )

dataproc_pyspark_1 = dataproc_operator.DataProcPySparkOperator(
    task_id='Load_BQ_spark_job_1',
    # call the py file for processing
    #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
    main='gs://worldbank2021/code/dataproc_load_bq.py',
    cluster_name='cluster-58-wb',
    region='us-east1',
    arguments=['wb_country_series_definition'],
    dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
)

dataproc_pyspark_2 = dataproc_operator.DataProcPySparkOperator(
    task_id='Load_BQ_spark_job_2',
    main='gs://worldbank2021/code/dataproc_load_bq.py',
    cluster_name='cluster-58-wb',
    region='us-east1',
    arguments=['wb_country_summary'],
    dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
)

dataproc_pyspark_3 = dataproc_operator.DataProcPySparkOperator(
    task_id='Load_BQ_spark_job_3',
    # call the py file for processing
    #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
    main='gs://worldbank2021/code/dataproc_load_bq.py',
    cluster_name='cluster-58-wb',
    region='us-east1',
    arguments=['wb_health_nutrition_population'],
    dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
)

dataproc_pyspark_4 = dataproc_operator.DataProcPySparkOperator(
    task_id='Load_BQ_spark_job_4',
    # call the py file for processing
    #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
    main='gs://worldbank2021/code/dataproc_load_bq.py',
    cluster_name='cluster-58-wb',
    region='us-east1',
    arguments=['wb_series_summary'],
    dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
)

dataproc_pyspark_5 = dataproc_operator.DataProcPySparkOperator(
    task_id='Load_BQ_spark_job_5',
    # call the py file for processing
    #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
    main='gs://worldbank2021/code/dataproc_load_bq.py',
    cluster_name='cluster-58-wb',
    region='us-east1',
    arguments=['wb_series_times'],
    dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
)

# create_dataproc_cluster >> dataproc_etl >> dataproc_pyspark >> delete_dataproc_cluster
# create_dataproc_cluster >> etl_process >> dataproc_pyspark >> delete_dataproc_cluster
create_dataproc_cluster >> dataproc_pyspark_1
create_dataproc_cluster >> dataproc_pyspark_2
create_dataproc_cluster >> dataproc_pyspark_3
create_dataproc_cluster >> dataproc_pyspark_4
create_dataproc_cluster >> dataproc_pyspark_5
