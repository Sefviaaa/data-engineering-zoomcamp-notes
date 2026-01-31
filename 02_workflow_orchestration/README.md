# 02_workflow_orchestration - Workflow Orchestration with Airflow

This module covers workflow orchestration concepts and hands-on implementation using Apache Airflow, as part of the Data Engineering Zoomcamp.

## Contents

| File/Folder | Description |
|-------------|-------------|
| `airflow/` | Complete Airflow setup with Docker Compose |
| `data_ingestion_local.py` | Local data ingestion script |
| `ingest_script.py` | Ingestion utilities |
| `Homework/` | Homework solutions |

## Airflow Setup

The `airflow/` folder contains:

| File | Description |
|------|-------------|
| `docker-compose.yaml` | Airflow services (webserver, scheduler, workers) |
| `Dockerfile` | Custom Airflow image with dependencies |
| `requirements.txt` | Python dependencies |
| `dags/` | DAG definitions for data ingestion |
| `values.yaml` | Helm chart values for Kubernetes deployment |

## Quick Start

### 1. Start Airflow

```bash
cd airflow
docker-compose up -d
```

### 2. Access Airflow UI

Open [http://localhost:8080](http://localhost:8080) (default credentials: `airflow` / `airflow`)

### 3. Available DAGs

- `data_ingestion_gcp_yellow.py` - Ingest yellow taxi data to GCS/BigQuery
- `data_ingestion_gcp_green.py` - Ingest green taxi data to GCS/BigQuery
- `data_ingestion_local.py` - Ingest data to local PostgreSQL

## Topics Covered

- **Apache Airflow**: DAGs, operators, sensors, connections
- **Docker Compose**: Multi-container orchestration for Airflow
- **GCP Integration**: GCS uploads, BigQuery loading
- **Scheduling**: Cron expressions, backfills

## Homework

See the [Homework](./Homework) folder for homework solutions.
