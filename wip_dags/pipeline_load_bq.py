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
    'start_date': datetime.datetime(2021, 1, 14),
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

    def greeting():
        import logging
        logging.info('Hello DataPipeline for World Health Data!')

    # An instance of an operator is called a task. In this case, the
    # hello_python task calls the "greeting" Python function.
    hello_python = python_operator.PythonOperator(
        task_id='KickOff',
        python_callable=greeting)

    dataproc_pyspark_1 = dataproc_operator.DataProcPySparkOperator(
        task_id = 'Load_BQ_spark_job_1',
        #call the py file for processing
    #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
        main='gs://worldbank2021/code/dataproc_load_bq.py',
        cluster_name='cluster-58-wb',
        region='us-east1',
        arguments=['wh_country_series_definition'],
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
    )

    dataproc_pyspark_2 = dataproc_operator.DataProcPySparkOperator(
        task_id='Load_BQ_spark_job_2',
        main='gs://worldbank2021/code/dataproc_load_bq.py',
        cluster_name='cluster-58-wb',
        region='us-east1',
        arguments=['wh_country_summary'],
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
    )

    dataproc_pyspark_3 = dataproc_operator.DataProcPySparkOperator(
        task_id='Load_BQ_spark_job_3',
        # call the py file for processing
        #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
        main='gs://worldbank2021/code/dataproc_load_bq.py',
        cluster_name='cluster-58-wb',
        region='us-east1',
        arguments=['wh_health_nutrition_population'],
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
    )

    dataproc_pyspark_4 = dataproc_operator.DataProcPySparkOperator(
        task_id='Load_BQ_spark_job_4',
        # call the py file for processing
        #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
        main='gs://worldbank2021/code/dataproc_load_bq.py',
        cluster_name='cluster-58-wb',
        region='us-east1',
        arguments=['wh_series_summary'],
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
    )

    dataproc_pyspark_5 = dataproc_operator.DataProcPySparkOperator(
        task_id='Load_BQ_spark_job_5',
        # call the py file for processing
        #    main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_wb.py',
        main='gs://worldbank2021/code/dataproc_load_bq.py',
        cluster_name='cluster-58-wb',
        region='us-east1',
        arguments=['wh_series_times'],
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar']
    )


    #create_dataproc_cluster >> dataproc_etl >> dataproc_pyspark >> delete_dataproc_cluster
    #create_dataproc_cluster >> etl_process >> dataproc_pyspark >> delete_dataproc_cluster
    hello_python >> dataproc_pyspark_1
    hello_python >> dataproc_pyspark_2
    hello_python >> dataproc_pyspark_3
    hello_python >> dataproc_pyspark_4
    hello_python >> dataproc_pyspark_5
