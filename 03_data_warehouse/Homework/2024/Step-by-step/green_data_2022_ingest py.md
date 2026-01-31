# green_data_2022_ingest.py

```sql
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import pyarrow.csv as pv
import pyarrow.parquet as pq
import logging

# Make sure the values match your gcp values
PROJECT_ID="noted-aloe-481504-u4"
BUCKET="zoomcamp-datalake-sefvia"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def download_upload_tripdata(
        dag,
        dataset,
        url_template,
        gcs_prefix,
):
    
    def download(execution_date, **context):
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")

        filename = f"{dataset}_tripdata_{year}-{month}.parquet"
        url = url_template.format(year=year, month=month)
        local_path = f"{AIRFLOW_HOME}/{filename}"

        # Download the file
        response = requests.get(url, stream=True)
        response.raise_for_status()
        logging.info(f"Downloading {filename}...")

        with open(local_path, "wb") as f:
            for chunk in response.iter_content(1024 * 1024):
                f.write(chunk)
        logging.info(f"Downloaded {filename} to {local_path}")

    def upload_to_gcs(execution_date, **context):
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")

        filename = f"{dataset}_tripdata_{year}-{month}.parquet"
        local_path = f"{AIRFLOW_HOME}/{filename}"
        gcs_path = f"{gcs_prefix}/{year}/{filename}"

        # Upload to GCS
        logging.info(f"Uploading {filename} to GCS at {gcs_path}...")
        hook = GCSHook(gcp_conn_id="gcp-airflow")
        hook.upload(
            bucket_name=BUCKET,
            object_name=gcs_path,
            filename=local_path,
        )
        logging.info(f"Uploaded {filename} to GCS at {gcs_path}")
        
        os.remove(local_path)
        logging.info(f"Removed local file {local_path}")

    with dag:
        download_task = PythonOperator(
            task_id=f"download_{dataset}_tripdata",
            python_callable=download,
        )

        upload_task = PythonOperator(
            task_id=f"upload_{dataset}_tripdata",
            python_callable=upload_to_gcs,
        )

        download_task >> upload_task

green_dag = DAG(
    "GCP_ingestion_green",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 12, 31),
    catchup=True, 
    max_active_runs=3,
)

download_upload_tripdata(
    dag=green_dag,
    dataset="green",
    url_template="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet",
    gcs_prefix="raw/green"
)

```