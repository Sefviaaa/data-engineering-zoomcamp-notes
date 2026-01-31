# Homework Solutions - Module 01: Docker & Terraform

Yearly homework solutions for the Data Engineering Zoomcamp Week 1.

## Solutions by Year

| Year | Description |
|------|-------------|
| [2022](./2022.md) | PostgreSQL, Terraform basics, SQL queries |
| [2023](./2023.md) | Updated homework with new datasets |
| [2024](./2024.md) | Terraform variables, data ingestion |
| [2025](./2025.md) | Modern Docker practices |
| [2026](./2026.md) | Latest homework solutions |

## Supporting Files

| File | Description |
|------|-------------|
| `Dockerfile` | Container configuration for ingestion scripts |
| `docker-compose.yaml` | PostgreSQL + pgAdmin setup |
| `ingest.ipynb` | Jupyter notebook for data exploration |
| `ingest_data20XX.py` | Year-specific data ingestion scripts (uses `click` CLI) |

## Quick Start

### 1. Start PostgreSQL and pgAdmin

```bash
docker-compose up -d
```

### 2. Build the Ingestion Image

```bash
docker build -t taxi_ingest_hw:v001 .
```

### 3. Run Data Ingestion (2026 Example)

```bash
docker run -it --network=host taxi_ingest_hw:v001 \
  --pg_user=root \
  --pg_pass=root \
  --pg_host=localhost \
  --pg_port=5432 \
  --pg_db=ny_taxi \
  --pg_year=2026 \
  --pg_month=1 \
  --ingest_zones
```

### Arguments (2026 Script)

| Argument | Description | Default |
|----------|-------------|---------|
| `--pg_user` | PostgreSQL username | `root` |
| `--pg_pass` | PostgreSQL password | `root` |
| `--pg_host` | PostgreSQL host | `localhost` |
| `--pg_port` | PostgreSQL port | `5432` |
| `--pg_db` | Database name | `ny_taxi` |
| `--pg_year` | Year of data to ingest | `2026` |
| `--pg_month` | Month of data to ingest | `1` |
| `--chunk_size` | Rows per batch | `100000` |
| `--ingest_zones` | Also ingest taxi zone lookup table | flag |

## Topics Covered in Homework

- Setting up Google Cloud SDK
- Terraform `init`, `plan`, `apply` workflow
- PostgreSQL data loading with Docker
- SQL queries (aggregations, joins, window functions)
- Docker containerization of Python scripts
