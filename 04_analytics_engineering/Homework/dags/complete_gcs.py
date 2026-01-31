import os
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pyarrow as pa
import pyarrow.parquet as pq

import requests
import logging

GREEN_SCHEMA = pa.schema([
    ("VendorID", pa.int64()),
    ("lpep_pickup_datetime", pa.timestamp("us")),
    ("lpep_dropoff_datetime", pa.timestamp("us")),
    ("store_and_fwd_flag", pa.string()),
    ("RatecodeID", pa.int64()),
    ("PULocationID", pa.int64()),
    ("DOLocationID", pa.int64()),
    pa.field("passenger_count", pa.int64(), nullable=True),
    ("trip_distance", pa.float64()),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("ehail_fee", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    pa.field("payment_type", pa.int64(), nullable=True),
    pa.field("trip_type", pa.int64(), nullable=True),
    ("congestion_surcharge", pa.float64()),
])

YELLOW_SCHEMA = pa.schema([
    ("VendorID", pa.int64()),
    ("tpep_pickup_datetime", pa.timestamp("us")),
    ("tpep_dropoff_datetime", pa.timestamp("us")),
    pa.field("passenger_count", pa.int64(), nullable=True),
    ("trip_distance", pa.float64()),
    pa.field("RatecodeID", pa.int64(), nullable=True),
    ("store_and_fwd_flag", pa.string()),
    ("PULocationID", pa.int64()),
    ("DOLocationID", pa.int64()),
    pa.field("payment_type", pa.int64(), nullable=True),
    ("fare_amount", pa.float64()),
    ("extra", pa.float64()),
    ("mta_tax", pa.float64()),
    ("tip_amount", pa.float64()),
    ("tolls_amount", pa.float64()),
    ("improvement_surcharge", pa.float64()),
    ("total_amount", pa.float64()),
    ("congestion_surcharge", pa.float64()),
])

FHV_SCHEMA = pa.schema([
    ("dispatching_base_num", pa.string()),
    ("pickup_datetime", pa.timestamp("us")),
    ("dropOff_datetime", pa.timestamp("us")),
    ("PUlocationID", pa.float64()),
    ("DOlocationID", pa.float64()),
    ("SR_Flag", pa.float64()), 
    pa.field("Affiliated_base_number", pa.string(), nullable=True),
])


# Make sure the values ​​match your gcp values
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

        table = pq.read_table(local_path)

        if dataset == "green":
            table = align_table_to_schema(table, GREEN_SCHEMA)
            table = table.cast(GREEN_SCHEMA, safe=False)

        elif dataset == "yellow":
            table = align_table_to_schema(table, YELLOW_SCHEMA)
            table = table.cast(YELLOW_SCHEMA, safe=False)

        elif dataset == "fhv":
            table = align_table_to_schema(table, FHV_SCHEMA)
            table = table.cast(FHV_SCHEMA, safe=True)

        pq.write_table(table, local_path, compression="snappy")


        logging.info(f"Rewritten {filename} with fixed schema")

    def align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
        for field in schema:
            if field.name not in table.schema.names:
                table = table.append_column(
                    field.name,
                    pa.nulls(len(table), type=field.type)
                )
        return table.select(schema.names)

    
    def upload_to_gcs(execution_date, **context):
        year = execution_date.strftime("%Y")
        month = execution_date.strftime("%m")

        filename = f"{dataset}_tripdata_{year}-{month}.parquet"
        local_path = f"{AIRFLOW_HOME}/{filename}"
        gcs_path = f"{gcs_prefix}/{year}/{filename}"

        hook = GCSHook(gcp_conn_id="gcp-airflow")

        hook.upload(
            bucket_name=BUCKET,
            object_name=gcs_path,
            filename=local_path,
            timeout=600,      # 🔥 THIS FIXES YOUR TIMEOUT
        )

        os.remove(local_path)


    with dag:
        download_task = PythonOperator(
            task_id=f"download_{dataset}_tripdata",
            python_callable=download,
        )

        upload_task = PythonOperator(
            task_id=f"upload_{dataset}_tripdata",
            python_callable=upload_to_gcs,
        )

        create_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f"create_external_table_{dataset}",


            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": "nytaxi",
                    "tableId": f"{dataset}_tripdata_external_{{{{ execution_date.year }}}}",
                },
                "externalDataConfiguration": {
                    "sourceFormat": "PARQUET",
                    "sourceUris": [
                        f"gs://{BUCKET}/{gcs_prefix}/{{{{ execution_date.year }}}}/*.parquet"
                    ],
                },
            },
            gcp_conn_id="gcp-airflow",
        )

        download_task >> upload_task >> create_external_table_task  



green_dag = DAG(
    "GCP_ingestion_green_gcs",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
    catchup=True, 
    max_active_runs=3,
    default_args={
        "depends_on_past": True,
    },
)

download_upload_tripdata(
    dag=green_dag,
    dataset="green",
    url_template="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet",
    gcs_prefix="raw/green"
)

yellow_dag = DAG(
    "GCP_ingestion_yellow_gcs",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31),
    catchup=True, 
    max_active_runs=3,
    default_args={
        "depends_on_past": True,
    },
)

download_upload_tripdata(
    dag=yellow_dag,
    dataset="yellow",
    url_template="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet",
    gcs_prefix="raw/yellow"
)

fhv_dag = DAG(
    "GCP_ingestion_fhv_gcs",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 31),
    catchup=True, 
    max_active_runs=3,
    default_args={
        "depends_on_past": True,
    },
)

download_upload_tripdata(
    dag=fhv_dag,
    dataset="fhv",
    url_template="https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{year}-{month}.parquet",
    gcs_prefix="raw/fhv"
)