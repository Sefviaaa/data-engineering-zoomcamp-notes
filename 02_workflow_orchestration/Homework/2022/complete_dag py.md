# complete_dag.py

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

ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONES_CSV = "taxi_zone_lookup.csv"
ZONES_PARQUET = "taxi_zone_lookup.parquet"

def download_upload_zone(dag,gcs_prefix):
    
    def download():

        local_csv = f"{AIRFLOW_HOME}/{ZONES_CSV}"
        # Download the CSV
        response = requests.get(ZONES_URL)
        response.raise_for_status()
        logging.info(f"Downloading {ZONES_CSV}...")

        with open(local_csv, "wb") as f:
            f.write(response.content)
        logging.info(f"Downloaded {ZONES_CSV} to {local_csv}")

    def convert_to_parquet():
        local_csv = f"{AIRFLOW_HOME}/{ZONES_CSV}"
        local_parquet = f"{AIRFLOW_HOME}/{ZONES_PARQUET}"
        
        table = pv.read_csv(local_csv)
        pq.write_table(table, local_parquet)
        logging.info(f"Converted {ZONES_CSV} to {ZONES_PARQUET}")

    def upload_to_gcs():
        local_parquet = f"{AIRFLOW_HOME}/{ZONES_PARQUET}"
        gcs_path = f"{gcs_prefix}/{ZONES_PARQUET}"
        local_csv = f"{AIRFLOW_HOME}/{ZONES_CSV}"

        hook = GCSHook(gcp_conn_id="gcp-airflow")
        hook.upload(
            bucket_name=BUCKET,
            object_name=gcs_path,
            filename=local_parquet,
            timeout=600
        )

        os.remove(local_parquet)
        os.remove(local_csv)
        logging.info(f"Uploaded {ZONES_PARQUET} to GCS at {gcs_path} and removed local file.")

    with dag:
        download_task = PythonOperator(
            task_id="download",
            python_callable=download,
            retries=10,
        )

        convert_to_parquet_task = PythonOperator(
            task_id="convert_to_parquet",
            python_callable=convert_to_parquet,
            retries=10,
        )

        upload_task = PythonOperator(
            task_id="upload_to_gcs",
            python_callable=upload_to_gcs,
            retries=10,
        )

        download_task >> convert_to_parquet_task >> upload_task

yellow_dag = DAG(
    "GCP_ingestion_yellow",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
    catchup=True, 
    max_active_runs=3,
)

download_upload_tripdata(
    dag=yellow_dag,
    dataset="yellow",
    url_template="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet",
    gcs_prefix="raw/yellow"
)

fhv_dag = DAG(
    "FHV_ingestion_DAG",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 31),
    catchup=True,
    max_active_runs=3,
)

download_upload_tripdata(
    dag=fhv_dag,
    dataset="fhv",
    url_template="https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet",
    gcs_prefix="raw/fhv"
)

zone_dag = DAG(
    "Zone_ingestion_DAG",
    schedule_interval="@once",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    max_active_runs=1,
)

download_upload_zone(
    dag=zone_dag,
    gcs_prefix="raw/zones"
)

```