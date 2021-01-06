# Google-Composer-Airflow-Datastore-to-Cloud-Storage

The following tasks must be done:

Task 1 - Create a python script which will run as a job in the data pipeline. The python script will read data from the nosql db and clean the data - remove null values from all columns, remove duplicate entries. The cleaned data is then written into parquet or orc file (alternatively can write to a json file).

Task 2 - Write another python script to read from the parquet or orc file and create a descriptive summary. Compute sum of a numeric column (for instance salary). You can take it a step further by grouping it on a specific column (eg: Title) and find the highest salaried Title.

Task 3 - Integrate the above jobs into a data pipeline, the job flow is important so step 2 should precede step 3 in the data pipeline 
