# Task 3 

Integrate the above jobs into a data pipeline, the job flow is important so step 2 should precede step 3 in the data pipeline 

Solution: Created a google composer with the following pypi packages as shown in screenshot below:

<kbd>
<img src="https://github.com/Sadiya-Dalvi/GoogleComposer/blob/main/Images/composer_with_pypi%20packages.png" alt="Cloud Composer" width="700" height="300" >
</kbd

/
  

Created the following datapipeline using google composer and apache airflow where the tasks are are given below:

1. Task id(create_dataproc_cluster) - Created a dataproc cluster 
2. Task id(etl_process) - Ran an ETL process as described in task 1. This calls etl_save.py.  
3. Task id(Query_Data_spark_job) - Ran 'dataproc_pyspark' job to show  descriptive summary and other computations. This calls the python script dataproc_nyctaxi.py. 
4. Task id (delete_dataproc_cluster) - Deleted the dataproc cluster 'delete_dataproc_cluster'

<kbd>
<img src="https://github.com/Sadiya-Dalvi/GoogleComposer/blob/main/Images/airflow.png" alt="Cloud Composer-airflow" width="700" height="300" >
</kbd
  
  /



The DAG  datapipeline.py is given below:

```
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
       cluster_name='cluster-58d6',
       num_workers=2,
       region='us-east1',
       master_machine_type='n1-standard-2',
       worker_machine_type='n1-standard-2'
    )

    etl_process = bash_operator.BashOperator(
        task_id='etl_process',
        # Run python file for ETL process.
        bash_command='python /home/airflow/gcs/data/etl_save.py')


    dataproc_pyspark = dataproc_operator.DataProcPySparkOperator(
        task_id = 'Query_Data_spark_job',
        #call the py file for processing
        main='gs://dataproc-nyc-taxi-2020/code_deploy/dataproc_nyctaxi.py',
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

  create_dataproc_cluster >> etl_process >> dataproc_pyspark >> delete_dataproc_cluster
 ```
  
  
  Output
  
<kbd>
<img src="https://github.com/Sadiya-Dalvi/GoogleComposer/blob/main/Images/dag_success.png" alt="Dag successful job" width="700" height="300" >
</kbd>

Output of Task_id = create_dataproc_cluster

```
*** Reading remote log from gs://us-central1-firstcomposer-12f2193c-bucket/logs/composer_datapipeline/create_dataproc_cluster/2021-01-05T00:00:00+00:00/1.log.
[2021-01-06 00:00:19,571] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.create_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 00:00:19,747] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.create_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 00:00:19,748] {taskinstance.py:881} INFO -
--------------------------------------------------------------------------------
[2021-01-06 00:00:19,748] {taskinstance.py:882} INFO - Starting attempt 1 of 1
[2021-01-06 00:00:19,748] {taskinstance.py:883} INFO -
--------------------------------------------------------------------------------
[2021-01-06 00:00:19,874] {taskinstance.py:902} INFO - Executing <Task(DataprocClusterCreateOperator): create_dataproc_cluster> on 2021-01-05T00:00:00+00:00
[2021-01-06 00:00:19,883] {standard_task_runner.py:54} INFO - Started process 3240 to run task
[2021-01-06 00:00:20,252] {standard_task_runner.py:77} INFO - Running: ['airflow', 'run', 'composer_datapipeline', 'create_dataproc_cluster', '2021-01-05T00:00:00+00:00', '--job_id', '3769', '--pool', 'default_pool', '--raw', '-sd', 'DAGS_FOLDER/datapipeline.py', '--cfg_path', '/tmp/tmp_pmlxz4g']
[2021-01-06 00:00:20,267] {standard_task_runner.py:78} INFO - Job 3769: Subtask create_dataproc_cluster
[2021-01-06 00:00:21,498] {logging_mixin.py:112} INFO - Running <TaskInstance: composer_datapipeline.create_dataproc_cluster 2021-01-05T00:00:00+00:00 [running]> on host airflow-worker-6c4b6b6fd4-rhfgj
[2021-01-06 00:00:22,055] {dataproc_operator.py:447} INFO - Creating cluster: cluster-58d6
[2021-01-06 00:00:24,879] {taskinstance.py:1152} ERROR - <HttpError 409 when requesting https://dataproc.googleapis.com/v1beta2/projects/dataproc-300110/regions/us-east1/clusters?requestId=367cd7f7-771a-4ab4-a73c-213a6299034e&alt=json returned "Already exists: Failed to create cluster: Cluster projects/dataproc-300110/regions/us-east1/clusters/cluster-58d6">
Traceback (most recent call last)
  File "/usr/local/lib/airflow/airflow/models/taskinstance.py", line 985, in _run_raw_tas
    result = task_copy.execute(context=context
  File "/usr/local/lib/airflow/airflow/contrib/operators/dataproc_operator.py", line 64, in execut
    self.hook.wait(self.start()
  File "/usr/local/lib/airflow/airflow/contrib/operators/dataproc_operator.py", line 455, in star
    requestId=str(uuid.uuid4())
  File "/opt/python3.6/lib/python3.6/site-packages/googleapiclient/_helpers.py", line 134, in positional_wrappe
    return wrapped(*args, **kwargs
  File "/opt/python3.6/lib/python3.6/site-packages/googleapiclient/http.py", line 907, in execut
    raise HttpError(resp, content, uri=self.uri
googleapiclient.errors.HttpError: <HttpError 409 when requesting https://dataproc.googleapis.com/v1beta2/projects/dataproc-300110/regions/us-east1/clusters?requestId=367cd7f7-771a-4ab4-a73c-213a6299034e&alt=json returned "Already exists: Failed to create cluster: Cluster projects/dataproc-300110/regions/us-east1/clusters/cluster-58d6"
[2021-01-06 00:00:24,914] {taskinstance.py:1196} INFO - Marking task as FAILED. dag_id=composer_datapipeline, task_id=create_dataproc_cluster, execution_date=20210105T000000, start_date=20210106T000019, end_date=20210106T000024
[2021-01-06 00:00:28,990] {local_task_job.py:102} INFO - Task exited with return code 1
[2021-01-06 05:02:12,181] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.create_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 05:02:12,368] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.create_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 05:02:12,371] {taskinstance.py:881} INFO -
--------------------------------------------------------------------------------
[2021-01-06 05:02:12,371] {taskinstance.py:882} INFO - Starting attempt 1 of 1
[2021-01-06 05:02:12,372] {taskinstance.py:883} INFO -
--------------------------------------------------------------------------------
[2021-01-06 05:02:12,486] {taskinstance.py:902} INFO - Executing <Task(DataprocClusterCreateOperator): create_dataproc_cluster> on 2021-01-05T00:00:00+00:00
[2021-01-06 05:02:12,512] {standard_task_runner.py:54} INFO - Started process 36283 to run task
[2021-01-06 05:02:12,615] {standard_task_runner.py:77} INFO - Running: ['airflow', 'run', 'composer_datapipeline', 'create_dataproc_cluster', '2021-01-05T00:00:00+00:00', '--job_id', '4147', '--pool', 'default_pool', '--raw', '-sd', 'DAGS_FOLDER/datapipeline.py', '--cfg_path', '/tmp/tmpnm9d9evl']
[2021-01-06 05:02:12,623] {standard_task_runner.py:78} INFO - Job 4147: Subtask create_dataproc_cluster
[2021-01-06 05:02:14,086] {logging_mixin.py:112} INFO - Running <TaskInstance: composer_datapipeline.create_dataproc_cluster 2021-01-05T00:00:00+00:00 [running]> on host airflow-worker-6c4b6b6fd4-rhfgj
[2021-01-06 05:02:14,496] {dataproc_operator.py:447} INFO - Creating cluster: cluster-58d6
[2021-01-06 05:02:17,602] {gcp_dataproc_hook.py:250} INFO - Waiting for Dataproc Operation projects/dataproc-300110/regions/us-east1/operations/c8bd5dea-426f-3c79-93d9-6c9db48f52de to finish
[2021-01-06 05:05:09,320] {gcp_dataproc_hook.py:275} INFO - Dataproc Operation projects/dataproc-300110/regions/us-east1/operations/c8bd5dea-426f-3c79-93d9-6c9db48f52de done
[2021-01-06 05:05:09,393] {taskinstance.py:1071} INFO - Marking task as SUCCESS.dag_id=composer_datapipeline, task_id=create_dataproc_cluster, execution_date=20210105T000000, start_date=20210106T050212, end_date=20210106T050509
[2021-01-06 05:05:13,520] {local_task_job.py:102} INFO - Task exited with return code 0
```
# Output of task_id= 'etl_process'

```
*** Reading remote log from gs://us-central1-firstcomposer-12f2193c-bucket/logs/composer_datapipeline/etl_process/2021-01-05T00:00:00+00:00/1.log.
[2021-01-06 05:05:29,889] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.etl_process 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 05:05:30,042] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.etl_process 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 05:05:30,042] {taskinstance.py:881} INFO -
--------------------------------------------------------------------------------
[2021-01-06 05:05:30,043] {taskinstance.py:882} INFO - Starting attempt 1 of 1
[2021-01-06 05:05:30,043] {taskinstance.py:883} INFO -
--------------------------------------------------------------------------------
[2021-01-06 05:05:30,101] {taskinstance.py:902} INFO - Executing <Task(BashOperator): etl_process> on 2021-01-05T00:00:00+00:00
[2021-01-06 05:05:30,110] {standard_task_runner.py:54} INFO - Started process 36316 to run task
[2021-01-06 05:05:30,174] {standard_task_runner.py:77} INFO - Running: ['airflow', 'run', 'composer_datapipeline', 'etl_process', '2021-01-05T00:00:00+00:00', '--job_id', '4149', '--pool', 'default_pool', '--raw', '-sd', 'DAGS_FOLDER/datapipeline.py', '--cfg_path', '/tmp/tmplo2xoq6b']
[2021-01-06 05:05:30,178] {standard_task_runner.py:78} INFO - Job 4149: Subtask etl_process
[2021-01-06 05:05:31,038] {logging_mixin.py:112} INFO - Running <TaskInstance: composer_datapipeline.etl_process 2021-01-05T00:00:00+00:00 [running]> on host airflow-worker-6c4b6b6fd4-rhfgj
[2021-01-06 05:05:31,178] {bash_operator.py:114} INFO - Tmp dir root location:
 /tmp
[2021-01-06 05:05:31,186] {bash_operator.py:137} INFO - Temporary script location: /tmp/airflowtmpa68o4f5e/etl_processooe9p9wp
[2021-01-06 05:05:31,186] {bash_operator.py:147} INFO - Running command: python /home/airflow/gcs/data/etl_save.py
[2021-01-06 05:05:31,371] {bash_operator.py:154} INFO - Output:
[2021-01-06 05:05:55,253] {bash_operator.py:158} INFO - DOLocationID             0.000000
[2021-01-06 05:05:55,256] {bash_operator.py:158} INFO - PULocationID             0.000279
[2021-01-06 05:05:55,257] {bash_operator.py:158} INFO - RatecodeID               0.000000
[2021-01-06 05:05:55,257] {bash_operator.py:158} INFO - VendorID                 0.000000
[2021-01-06 05:05:55,257] {bash_operator.py:158} INFO - extra                    0.000000
[2021-01-06 05:05:55,257] {bash_operator.py:158} INFO - fare_amount              0.000000
[2021-01-06 05:05:55,260] {bash_operator.py:158} INFO - improvement_surcharge    0.000000
[2021-01-06 05:05:55,260] {bash_operator.py:158} INFO - kind                     0.000000
[2021-01-06 05:05:55,261] {bash_operator.py:158} INFO - mta_tax                  0.000000
[2021-01-06 05:05:55,261] {bash_operator.py:158} INFO - passenger_count          0.000000
[2021-01-06 05:05:55,261] {bash_operator.py:158} INFO - payment_type             0.000279
[2021-01-06 05:05:55,261] {bash_operator.py:158} INFO - store_and_fwd_flag       0.000000
[2021-01-06 05:05:55,262] {bash_operator.py:158} INFO - tip_amount               0.000279
[2021-01-06 05:05:55,264] {bash_operator.py:158} INFO - tolls_amount             0.000000
[2021-01-06 05:05:55,264] {bash_operator.py:158} INFO - total_amount             0.000000
[2021-01-06 05:05:55,265] {bash_operator.py:158} INFO - tpep_dropoff_datetime    0.000000
[2021-01-06 05:05:55,265] {bash_operator.py:158} INFO - tpep_pickup_datetime     0.000000
[2021-01-06 05:05:55,265] {bash_operator.py:158} INFO - trip_distance            0.000558
[2021-01-06 05:05:55,268] {bash_operator.py:158} INFO - dtype: float64
[2021-01-06 05:05:55,269] {bash_operator.py:158} INFO - There are tolls_amount             3586
[2021-01-06 05:05:55,269] {bash_operator.py:158} INFO - store_and_fwd_flag       3586
[2021-01-06 05:05:55,269] {bash_operator.py:158} INFO - kind                     3586
[2021-01-06 05:05:55,269] {bash_operator.py:158} INFO - RatecodeID               3586
[2021-01-06 05:05:55,270] {bash_operator.py:158} INFO - mta_tax                  3586
[2021-01-06 05:05:55,272] {bash_operator.py:158} INFO - tpep_dropoff_datetime    3586
[2021-01-06 05:05:55,272] {bash_operator.py:158} INFO - passenger_count          3586
[2021-01-06 05:05:55,273] {bash_operator.py:158} INFO - fare_amount              3586
[2021-01-06 05:05:55,273] {bash_operator.py:158} INFO - PULocationID             3585
[2021-01-06 05:05:55,273] {bash_operator.py:158} INFO - total_amount             3586
[2021-01-06 05:05:55,273] {bash_operator.py:158} INFO - payment_type             3585
[2021-01-06 05:05:55,274] {bash_operator.py:158} INFO - extra                    3586
[2021-01-06 05:05:55,274] {bash_operator.py:158} INFO - tip_amount               3585
[2021-01-06 05:05:55,276] {bash_operator.py:158} INFO - improvement_surcharge    3586
[2021-01-06 05:05:55,276] {bash_operator.py:158} INFO - tpep_pickup_datetime     3586
[2021-01-06 05:05:55,276] {bash_operator.py:158} INFO - DOLocationID             3586
[2021-01-06 05:05:55,277] {bash_operator.py:158} INFO - trip_distance            3584
[2021-01-06 05:05:55,277] {bash_operator.py:158} INFO - VendorID                 3586
[2021-01-06 05:05:55,277] {bash_operator.py:158} INFO - dtype: int64 rows in the Dataframe.
[2021-01-06 05:05:55,327] {bash_operator.py:158} INFO - There are tolls_amount             3581
[2021-01-06 05:05:55,328] {bash_operator.py:158} INFO - store_and_fwd_flag       3581
[2021-01-06 05:05:55,328] {bash_operator.py:158} INFO - kind                     3581
[2021-01-06 05:05:55,329] {bash_operator.py:158} INFO - RatecodeID               3581
[2021-01-06 05:05:55,330] {bash_operator.py:158} INFO - mta_tax                  3581
[2021-01-06 05:05:55,331] {bash_operator.py:158} INFO - tpep_dropoff_datetime    3581
[2021-01-06 05:05:55,332] {bash_operator.py:158} INFO - passenger_count          3581
[2021-01-06 05:05:55,332] {bash_operator.py:158} INFO - fare_amount              3581
[2021-01-06 05:05:55,333] {bash_operator.py:158} INFO - PULocationID             3581
[2021-01-06 05:05:55,333] {bash_operator.py:158} INFO - total_amount             3581
[2021-01-06 05:05:55,333] {bash_operator.py:158} INFO - payment_type             3581
[2021-01-06 05:05:55,335] {bash_operator.py:158} INFO - extra                    3581
[2021-01-06 05:05:55,335] {bash_operator.py:158} INFO - tip_amount               3581
[2021-01-06 05:05:55,336] {bash_operator.py:158} INFO - improvement_surcharge    3581
[2021-01-06 05:05:55,336] {bash_operator.py:158} INFO - tpep_pickup_datetime     3581
[2021-01-06 05:05:55,336] {bash_operator.py:158} INFO - DOLocationID             3581
[2021-01-06 05:05:55,337] {bash_operator.py:158} INFO - trip_distance            3581
[2021-01-06 05:05:55,337] {bash_operator.py:158} INFO - VendorID                 3581
[2021-01-06 05:05:55,340] {bash_operator.py:158} INFO - dtype: int64 rows in the Dataframe after dropping Null.
[2021-01-06 05:05:55,361] {bash_operator.py:158} INFO - There are tolls_amount             3579
[2021-01-06 05:05:55,362] {bash_operator.py:158} INFO - store_and_fwd_flag       3579
[2021-01-06 05:05:55,364] {bash_operator.py:158} INFO - kind                     3579
[2021-01-06 05:05:55,365] {bash_operator.py:158} INFO - RatecodeID               3579
[2021-01-06 05:05:55,365] {bash_operator.py:158} INFO - mta_tax                  3579
[2021-01-06 05:05:55,365] {bash_operator.py:158} INFO - tpep_dropoff_datetime    3579
[2021-01-06 05:05:55,366] {bash_operator.py:158} INFO - passenger_count          3579
[2021-01-06 05:05:55,368] {bash_operator.py:158} INFO - fare_amount              3579
[2021-01-06 05:05:55,371] {bash_operator.py:158} INFO - PULocationID             3579
[2021-01-06 05:05:55,374] {bash_operator.py:158} INFO - total_amount             3579
[2021-01-06 05:05:55,375] {bash_operator.py:158} INFO - payment_type             3579
[2021-01-06 05:05:55,378] {bash_operator.py:158} INFO - extra                    3579
[2021-01-06 05:05:55,378] {bash_operator.py:158} INFO - tip_amount               3579
[2021-01-06 05:05:55,378] {bash_operator.py:158} INFO - improvement_surcharge    3579
[2021-01-06 05:05:55,379] {bash_operator.py:158} INFO - tpep_pickup_datetime     3579
[2021-01-06 05:05:55,379] {bash_operator.py:158} INFO - DOLocationID             3579
[2021-01-06 05:05:55,379] {bash_operator.py:158} INFO - trip_distance            3579
[2021-01-06 05:05:55,379] {bash_operator.py:158} INFO - VendorID                 3579
[2021-01-06 05:05:55,379] {bash_operator.py:158} INFO - dtype: int64 rows in the Dataframe after dropping duplicates.
[2021-01-06 05:06:02,003] {bash_operator.py:158} INFO - Copying file://yellowtaxi.parquet [Content-Type=application/octet-stream]...
[2021-01-06 05:06:02,886] {bash_operator.py:158} INFO - / [0 files][    0.0 B/103.2 KiB]                                                
/ [1 files][103.2 KiB/103.2 KiB]                                                
-
CommandException: No URLs matched: us-east1
[2021-01-06 05:06:06,360] {bash_operator.py:162} INFO - Command exited with return code 0
[2021-01-06 05:06:06,538] {taskinstance.py:1071} INFO - Marking task as SUCCESS.dag_id=composer_datapipeline, task_id=etl_process, execution_date=20210105T000000, start_date=20210106T050529, end_date=20210106T050606
[2021-01-06 05:06:10,107] {local_task_job.py:102} INFO - Task exited with return code 0

```
The parquet file is copied on to the cloud storage as part of task_id 'etl_process'
<kbd>
<img src="https://github.com/Sadiya-Dalvi/GoogleComposer/blob/main/Images/nyc_parquet.png" alt="parquet" width="700" height="300" >
</kbd>

Output of the dataproc job task_id = 'Query_Data_spark_job':

```
21/01/06 05:06:23 INFO org.spark_project.jetty.util.log: Logging initialized @3132ms
21/01/06 05:06:23 INFO org.spark_project.jetty.server.Server: jetty-9.3.z-SNAPSHOT, build timestamp: unknown, git hash: unknown
21/01/06 05:06:23 INFO org.spark_project.jetty.server.Server: Started @3223ms
21/01/06 05:06:23 INFO org.spark_project.jetty.server.AbstractConnector: Started ServerConnector@494ba188{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
21/01/06 05:06:24 WARN org.apache.spark.scheduler.FairSchedulableBuilder: Fair Scheduler configuration file not found so jobs will be scheduled in FIFO order. To use fair scheduling, configure pools in fairscheduler.xml or set spark.scheduler.allocation.file to a file that contains the configuration.
21/01/06 05:06:25 INFO org.apache.hadoop.yarn.client.RMProxy: Connecting to ResourceManager at cluster-58d6-m/10.142.0.30:8032
21/01/06 05:06:25 INFO org.apache.hadoop.yarn.client.AHSProxy: Connecting to Application History server at cluster-58d6-m/10.142.0.30:10200
21/01/06 05:06:28 INFO org.apache.hadoop.yarn.client.api.impl.YarnClientImpl: Submitted application application_1609909386446_0001
+-------+-------------------+------------------+------------------+------------------+-------------------+---------------------+------------------+------------------+------------------+------------------+------------------+-------------------+-----------------+---------------------+--------------------+------------------+-----------------+------------------+------------------+
|summary|       tolls_amount|store_and_fwd_flag|              kind|        RatecodeID|            mta_tax|tpep_dropoff_datetime|   passenger_count|       fare_amount|      PULocationID|      total_amount|      payment_type|              extra|       tip_amount|improvement_surcharge|tpep_pickup_datetime|      DOLocationID|    trip_distance|          VendorID| __index_level_0__|
+-------+-------------------+------------------+------------------+------------------+-------------------+---------------------+------------------+------------------+------------------+------------------+------------------+-------------------+-----------------+---------------------+--------------------+------------------+-----------------+------------------+------------------+
|  count|               3579|              3579|              3579|              3579|               3579|                 3579|              3579|              3579|              3579|              3579|              3579|               3579|             3579|                 3579|                3579|              3579|             3579|              3579|              3579|
|   mean|0.10788488404582276|              null|              null|1.0153674210673374| 0.4972059234423023|                 null|1.7692092763341716|12.291424979044425|161.91813355685946|15.397105336686648|1.3805532271584242|0.49427214305671974|1.690125733445098|  0.29891031014247815|                null|158.55490360435877|2.812713048337515| 1.609667504889634| 1794.506007264599|
| stddev| 0.8049131357562603|              null|              null|0.2167876091463735|0.04720250972245131|                 null|1.2496357056919436| 9.348342307273782| 70.06445769240358|10.712219163676831|0.5135629727163784|0.07593755144578695|2.300582644468607| 0.025053047183067317|                null| 74.87014650541457|3.065531100292209|0.5369794993892626|1034.1762705583787|
|    min|                  0|                 N|YellowTripdata2019|                 1|               -0.5|  2018-11-28 15:55:45|                 0|               -19|               100|             -20.3|                 1|               -0.5|                0|                 -0.3| 2018-11-28 15:52:25|                10|              .00|                 1|                 0|
|    max|               5.76|                 Y|YellowTripdata2019|                 5|                0.5|  2019-01-02 00:56:12|                 6|                92|                97|             99.06|                 4|                  3|             9.84|                  0.3| 2019-01-01 01:56:04|                98|             9.91|                 4|              3585|
+-------+-------------------+------------------+------------------+------------------+-------------------+---------------------+------------------+------------------+------------------+------------------+------------------+-------------------+-----------------+---------------------+--------------------+------------------+-----------------+------------------+------------------+

+------------------+
|avg(trip_distance)|
+------------------+
| 2.812713048337515|
+------------------+

+------------+------------------+------------------+----------+-------+---------------------+---------------+-----------+------------+------------+------------+-----+----------+---------------------+--------------------+------------+-------------+--------+-----------------+
|tolls_amount|store_and_fwd_flag|              kind|RatecodeID|mta_tax|tpep_dropoff_datetime|passenger_count|fare_amount|PULocationID|total_amount|payment_type|extra|tip_amount|improvement_surcharge|tpep_pickup_datetime|DOLocationID|trip_distance|VendorID|__index_level_0__|
+------------+------------------+------------------+----------+-------+---------------------+---------------+-----------+------------+------------+------------+-----+----------+---------------------+--------------------+------------+-------------+--------+-----------------+
|           0|                 N|YellowTripdata2019|         1|    0.5|  2019-01-01 00:53:20|              1|          7|         151|        9.95|           1|  0.5|      1.65|                  0.3| 2019-01-01 00:46:40|         239|         1.50|       1|                0|
|           0|                 N|YellowTripdata2019|         1|    0.5|  2019-01-01 01:18:59|              1|         14|         239|        16.3|           1|  0.5|         1|                  0.3| 2019-01-01 00:59:47|         246|         2.60|       1|                1|
|           0|                 N|YellowTripdata2019|         1|    0.5|  2018-12-21 13:52:40|              3|        4.5|         236|         5.8|           1|  0.5|         0|                  0.3| 2018-12-21 13:48:30|         236|          .00|       2|                2|
|           0|                 N|YellowTripdata2019|         1|    0.5|  2018-11-28 15:55:45|              5|        3.5|         193|        7.55|           2|  0.5|         0|                  0.3| 2018-11-28 15:52:25|         193|          .00|       2|                3|
|           0|                 N|YellowTripdata2019|         2|    0.5|  2018-11-28 15:58:33|              5|         52|         193|       55.55|           2|    0|         0|                  0.3| 2018-11-28 15:56:57|         193|          .00|       2|                4|
|        5.76|                 N|YellowTripdata2019|         1|    0.5|  2018-11-28 16:28:26|              5|        3.5|         193|       13.31|           2|  0.5|         0|                  0.3| 2018-11-28 16:25:49|         193|          .00|       2|                5|
|           0|                 N|YellowTripdata2019|         2|    0.5|  2018-11-28 16:33:43|              5|         52|         193|       55.55|           2|    0|         0|                  0.3| 2018-11-28 16:29:37|         193|          .00|       2|                6|
|           0|                 N|YellowTripdata2019|         1|    0.5|  2019-01-01 00:28:37|              1|        6.5|         163|        9.05|           1|  0.5|      1.25|                  0.3| 2019-01-01 00:21:28|         229|         1.30|       1|                7|
|           0|                 N|YellowTripdata2019|         1|    0.5|  2019-01-01 00:45:39|              1|       13.5|         229|        18.5|           1|  0.5|       3.7|                  0.3| 2019-01-01 00:32:01|           7|         3.70|       1|                8|
|           0|                 N|YellowTripdata2019|         1|    0.5|  2019-01-01 00:47:06|              2|         15|         246|       19.55|           1|  0.5|      3.25|                  0.3| 2019-01-01 00:24:04|         162|         2.80|       1|               10|
+------------+------------------+------------------+----------+-------+---------------------+---------------+-----------+------------+------------+------------+-----+----------+---------------------+--------------------+------------+-------------+--------+-----------------+
only showing top 10 rows

+---------------+-----+
|passenger_count|count|
+---------------+-----+
|              6|   93|
|              5|  145|
|              4|  134|
|              3|  237|
|              2|  869|
|              1| 2064|
|              0|   37|
+---------------+-----+

+---------------+------------------+
|passenger_count|avg(trip_distance)|
+---------------+------------------+
|              3|2.8803375527426174|
|              0|3.1432432432432433|
|              5|3.0468275862068963|
|              6|2.9978494623655902|
|              1|2.7496947674418597|
|              4|2.7372388059701493|
|              2|2.8826352128883785|
+---------------+------------------+

+---------------+-----+
|passenger_count|count|
+---------------+-----+
|              6|   93|
|              5|  145|
|              4|  134|
|              3|  237|
|              2|  869|
|              1| 2064|
|              0|   37|
+---------------+-----+

21/01/06 05:07:01 INFO org.spark_project.jetty.server.AbstractConnector: Stopped Spark@494ba188{HTTP/1.1,[http/1.1]}{0.0.0.0:4040}
Job output is complete
```


Output of task_id = delete_dataproc_cluster

```
*** Reading remote log from gs://us-central1-firstcomposer-12f2193c-bucket/logs/composer_datapipeline/delete_dataproc_cluster/2021-01-05T00:00:00+00:00/1.log.
[2021-01-06 00:00:11,732] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.delete_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 00:00:11,797] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.delete_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 00:00:11,797] {taskinstance.py:881} INFO -
--------------------------------------------------------------------------------
[2021-01-06 00:00:11,797] {taskinstance.py:882} INFO - Starting attempt 1 of 1
[2021-01-06 00:00:11,798] {taskinstance.py:883} INFO -
--------------------------------------------------------------------------------
[2021-01-06 00:00:11,849] {taskinstance.py:902} INFO - Executing <Task(DataprocClusterDeleteOperator): delete_dataproc_cluster> on 2021-01-05T00:00:00+00:00
[2021-01-06 00:00:11,854] {standard_task_runner.py:54} INFO - Started process 3407 to run task
[2021-01-06 00:00:11,888] {standard_task_runner.py:77} INFO - Running: ['airflow', 'run', 'composer_datapipeline', 'delete_dataproc_cluster', '2021-01-05T00:00:00+00:00', '--job_id', '3768', '--pool', 'default_pool', '--raw', '-sd', 'DAGS_FOLDER/datapipeline.py', '--cfg_path', '/tmp/tmpb0kx8r1e']
[2021-01-06 00:00:11,891] {standard_task_runner.py:78} INFO - Job 3768: Subtask delete_dataproc_cluster
[2021-01-06 00:00:12,645] {logging_mixin.py:112} INFO - Running <TaskInstance: composer_datapipeline.delete_dataproc_cluster 2021-01-05T00:00:00+00:00 [running]> on host airflow-worker-6c4b6b6fd4-cmq6t
[2021-01-06 00:00:12,730] {dataproc_operator.py:620} INFO - Deleting cluster: cluster-58d6 in us-east1
[2021-01-06 00:00:13,318] {gcp_dataproc_hook.py:250} INFO - Waiting for Dataproc Operation projects/dataproc-300110/regions/us-east1/operations/507288df-1d69-3642-8e03-c642ed9019ce to finish
[2021-01-06 00:00:43,765] {gcp_dataproc_hook.py:275} INFO - Dataproc Operation projects/dataproc-300110/regions/us-east1/operations/507288df-1d69-3642-8e03-c642ed9019ce done
[2021-01-06 00:00:43,808] {taskinstance.py:1071} INFO - Marking task as SUCCESS.dag_id=composer_datapipeline, task_id=delete_dataproc_cluster, execution_date=20210105T000000, start_date=20210106T000011, end_date=20210106T000043
[2021-01-06 00:00:46,542] {local_task_job.py:102} INFO - Task exited with return code 0
[2021-01-06 05:07:22,276] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.delete_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 05:07:22,608] {taskinstance.py:671} INFO - Dependencies all met for <TaskInstance: composer_datapipeline.delete_dataproc_cluster 2021-01-05T00:00:00+00:00 [queued]>
[2021-01-06 05:07:22,608] {taskinstance.py:881} INFO -
--------------------------------------------------------------------------------
[2021-01-06 05:07:22,609] {taskinstance.py:882} INFO - Starting attempt 1 of 1
[2021-01-06 05:07:22,609] {taskinstance.py:883} INFO -
--------------------------------------------------------------------------------
[2021-01-06 05:07:22,741] {taskinstance.py:902} INFO - Executing <Task(DataprocClusterDeleteOperator): delete_dataproc_cluster> on 2021-01-05T00:00:00+00:00
[2021-01-06 05:07:22,838] {standard_task_runner.py:54} INFO - Started process 36562 to run task
[2021-01-06 05:07:23,134] {standard_task_runner.py:77} INFO - Running: ['airflow', 'run', 'composer_datapipeline', 'delete_dataproc_cluster', '2021-01-05T00:00:00+00:00', '--job_id', '4152', '--pool', 'default_pool', '--raw', '-sd', 'DAGS_FOLDER/datapipeline.py', '--cfg_path', '/tmp/tmpblahhfpk']
[2021-01-06 05:07:23,144] {standard_task_runner.py:78} INFO - Job 4152: Subtask delete_dataproc_cluster
[2021-01-06 05:07:24,481] {logging_mixin.py:112} INFO - Running <TaskInstance: composer_datapipeline.delete_dataproc_cluster 2021-01-05T00:00:00+00:00 [running]> on host airflow-worker-6c4b6b6fd4-rhfgj
[2021-01-06 05:07:24,755] {dataproc_operator.py:620} INFO - Deleting cluster: cluster-58d6 in us-east1
[2021-01-06 05:07:25,933] {gcp_dataproc_hook.py:250} INFO - Waiting for Dataproc Operation projects/dataproc-300110/regions/us-east1/operations/ae77f0d9-ba08-357d-be35-5e9838d6f2a1 to finish
[2021-01-06 05:07:56,566] {gcp_dataproc_hook.py:275} INFO - Dataproc Operation projects/dataproc-300110/regions/us-east1/operations/ae77f0d9-ba08-357d-be35-5e9838d6f2a1 done
[2021-01-06 05:07:56,645] {taskinstance.py:1071} INFO - Marking task as SUCCESS.dag_id=composer_datapipeline, task_id=delete_dataproc_cluster, execution_date=20210105T000000, start_date=20210106T050722, end_date=20210106T050756
[2021-01-06 05:08:01,924] {local_task_job.py:102} INFO - Task exited with return code 0

```
