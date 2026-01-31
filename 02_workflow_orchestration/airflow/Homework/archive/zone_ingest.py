import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pyarrow.csv as pv
import pyarrow.parquet as pq
import logging



# Make sure the values ​​match your gcp values
PROJECT_ID="noted-aloe-481504-u4"
BUCKET="zoomcamp-datalake-sefvia"
BIGQUERY_DATASET = "airflow2025b"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
CSV_NAME = "taxi_zone_lookup.csv"
PARQUET_NAME = "taxi_zone_lookup.parquet"
URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


# Utility functions
def download():
    local_csv = f"{AIRFLOW_HOME}/{CSV_NAME}"
    # Download the CSV
    response = requests.get(URL)
    response.raise_for_status()
    logging.info(f"Downloading {CSV_NAME}...")

    with open(local_csv, "wb") as f:
        f.write(response.content)
    logging.info(f"Downloaded {CSV_NAME} to {AIRFLOW_HOME}/{CSV_NAME}")
    

def convert_to_parquet():
    local_csv = f"{AIRFLOW_HOME}/{CSV_NAME}"
    local_parquet = f"{AIRFLOW_HOME}/{PARQUET_NAME}"

    table = pv.read_csv(local_csv)
    pq.write_table(table, local_parquet)
    logging.info(f"Converted {CSV_NAME} to {PARQUET_NAME}")

def upload_to_gcs():
    local_parquet = f"{AIRFLOW_HOME}/{PARQUET_NAME}"
    gcs_path = f"raw/zones/{PARQUET_NAME}"
    local_csv = f"{AIRFLOW_HOME}/{CSV_NAME}"

    hook = GCSHook(gcp_conn_id="gcp-airflow")
    hook.upload(
        bucket_name=BUCKET,
        object_name=gcs_path,
        filename=local_parquet,
        timeout=600
    )

    os.remove(local_parquet)
    os.remove(local_csv)
    logging.info(f"Uploaded {PARQUET_NAME} to GCS at {gcs_path} and removed local file.")

# Defining the DAG
dag = DAG(
    "Zone_Ingest_DAG",
    schedule_interval="@once",
    start_date=datetime(2019, 1, 1),
    catchup=False, 
    max_active_runs=1,
)

# Task 1: Download and unzip file
download_task = PythonOperator(
    task_id="download",
    python_callable=download,
    retries=10,
    dag=dag
)

# Task 2: Format to parquet
format_to_parquet_task = PythonOperator(
    task_id="format_to_parquet",
    python_callable=convert_to_parquet,
    retries=10,
    dag=dag
)


# Task 3: Upload file to google storage
local_to_gcs_task = PythonOperator(
    task_id="upload_to_gcs",
    python_callable=upload_to_gcs,
    retries=10,
    dag=dag
)


download_task >> format_to_parquet_task >> local_to_gcs_task