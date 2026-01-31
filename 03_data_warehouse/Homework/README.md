# Homework Solutions - Module 03: Data Warehouse

Yearly homework solutions for the Data Engineering Zoomcamp Week 3.

## Solutions by Year

| Year | Description |
|------|-------------|
| [2024](./2024.md) | BigQuery, data ingestion with Airflow |
| [2025](./2025.md) | Latest homework solutions |

## Supporting Files

| File/Folder | Description |
|-------------|-------------|
| `Dockerfile` | Airflow container configuration |
| `docker-compose.yaml` | Airflow services setup |
| `dags/` | Airflow DAGs for data ingestion |
| `dags_new/` | Updated DAG implementations |
| `archive/` | Previous DAG versions |

## Quick Start

### 1. Start Airflow

```bash
docker-compose up -d
```

### 2. Access Airflow UI

Open [http://localhost:8080](http://localhost:8080)

### 3. Available DAGs

- `green_data_2022_ingest.py` - Ingest 2022 green taxi data
- `green_data_2023_gcs.py` - Ingest 2023 green taxi data to GCS
- `yellow_data_2024_jan_june.py` - Ingest Jan-June 2024 yellow taxi data

## Topics Covered

- Loading data into BigQuery using Airflow
- Partitioning and clustering tables
- Querying partitioned data efficiently
- Cost estimation in BigQuery
