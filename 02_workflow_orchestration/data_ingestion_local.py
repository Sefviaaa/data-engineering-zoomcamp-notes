import os
from datetime import datetime
from airflow import DAG

from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from ingest_script import ingest_callable  ## import the function from the ingest_script.py file

local_workflow = DAG(
    "LocalIngestionDag"
    schedule_interval= "0 6 2 * *",  ## this means at 6:00 am on the 2nd of every month
    start_date= datetime(2021,1,1)
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/") ## get the AIRFLOW_HOME environment variable, if not set, default to /opt/airflow/

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

## we need to create 2 task: wget and ingest

URL_PREFIX = 'https://s3.amazonaws.com/nyc-t1c/trip+data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' ## this is a jinja template, it will be rendered at runtime, so for each execution date, it will generate the correct url
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv'  ## this is also a jinja template, it will be rendered at runtime, so for each execution date, it will generate the correct output file name
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'  ## table name template

with local_workflow:
    wget_task = BashOperator(   ## we need to specify the operator, for the bashoperator we need task_id and bash_command
        task_id = 'wget'
        bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'  ## download the file from the url and save it to the output file
        # -O means output to a file, by default it will save to the same filename as in the url
    )

    ## make another one

    ingest_task = PythonOperator(  ## we need to specify the operator, for the pythonoperator we need task_id and python_callable
        task_id = 'ingest'
        python_callable = ingest_callable ## this is the function that will be called when the task is executed
        op_kwargs = dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE
            csv_file= OUTPUT_FILE_TEMPLATE

        )
    )
    ## now we need to specify the dependencies / order of execution. e.g ingest depends on wget, so wget should be executed first
    wget_task >> ingest_task  ## this means wget_task should be executed before ingest_task

