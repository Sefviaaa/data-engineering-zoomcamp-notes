import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import logging
from pathlib import Path


# Make sure the values ​​match your gcp values
PROJECT_ID="noted-aloe-481504-u4"
BUCKET="zoomcamp-datalake-sefvia"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


# Utility functions
def download(execution_date, **context):
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")

    filename = f"yellow_tripdata_{year}-{month}.parquet"
    url_template = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    local_path = f"{AIRFLOW_HOME}/{filename}"
    gcs_path = f"raw/yellow/{filename}"


    # Download the CSV.GZ file
    response = requests.get(url_template)
    response.raise_for_status()
    logging.info(f"Downloading {filename}...")

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    logging.info(f"Downloaded {filename} successfully.")


def upload_to_gcs(execution_date, **context):
    year = execution_date.strftime("%Y")
    month = execution_date.strftime("%m")

    filename = f"yellow_tripdata_{year}-{month}.parquet"
    local_path = f"{AIRFLOW_HOME}/{filename}"
    gcs_path = f"raw/yellow/{filename}"

    hook = GCSHook(gcp_conn_id="gcp-airflow")
    hook.upload(
        bucket_name=BUCKET,
        object_name=gcs_path,
        filename=local_path,
        timeout=600
    )

    os.remove(local_path)
    logging.info(f"Uploaded {filename} to GCS and removed local file.")


# Defining the DAG
dag = DAG(
    "GCP_ingestion_yellow",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
    catchup=True, 
    max_active_runs=1,
)

# Task 1: Download and unzip file
download_task = PythonOperator(
    task_id="download",
    python_callable=download,
    retries=10,
    dag=dag
)

# Task 2: Upload file to google storage
local_to_gcs_task = PythonOperator(
    task_id="upload_to_gcs",
    python_callable=upload_to_gcs,
    retries=10,
    dag=dag
)


download_task >> local_to_gcs_task
