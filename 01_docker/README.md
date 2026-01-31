# 01_docker - Docker & Terraform Fundamentals

This module covers the foundational concepts of containerization with Docker and infrastructure as code with Terraform, as part of the Data Engineering Zoomcamp.

## Contents

| File | Description |
|------|-------------|
| `Dockerfile` | Docker image configuration for data ingestion pipeline |
| `docker-compose.yaml` | Multi-container setup (PostgreSQL + pgAdmin) |
| `ingest_data.py` | Python script to ingest NYC taxi data into PostgreSQL |
| `pipeline.py` | Sample data pipeline script |
| `upload-data.ipynb` | Jupyter notebook for data exploration and upload |

## Topics Covered

- **Docker**: Containerization, Dockerfile creation, Docker networking
- **Docker Compose**: Multi-container orchestration
- **PostgreSQL**: Database setup and data ingestion
- **Terraform**: Infrastructure as Code basics (GCS, BigQuery provisioning)
- **pgAdmin**: Database management and SQL querying

## Quick Start

### 1. Start PostgreSQL and pgAdmin

```bash
docker-compose up -d
```

### 2. Build the Ingestion Image

```bash
docker build -t taxi_ingest:v001 .
```

### 3. Run Data Ingestion

```bash
docker run -it --network=host taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_data \
  --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
```

### Arguments

| Argument | Description |
|----------|-------------|
| `--user` | PostgreSQL username |
| `--password` | PostgreSQL password |
| `--host` | PostgreSQL host |
| `--port` | PostgreSQL port |
| `--db` | Database name |
| `--table_name` | Target table name |
| `--url` | URL of the CSV/gzip file to ingest |

## Homework

See the [Homework](./Homework) folder for yearly homework solutions and detailed walkthroughs.
