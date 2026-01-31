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

```bash
# Start PostgreSQL and pgAdmin
docker-compose up -d

# Run data ingestion
python ingest_data.py
```

## Homework

See the [Homework](./Homework) folder for yearly homework solutions and detailed walkthroughs.
