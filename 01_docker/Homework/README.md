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
| `ingest_data20XX.py` | Year-specific data ingestion scripts |

## Topics Covered in Homework

- Setting up Google Cloud SDK
- Terraform `init`, `plan`, `apply` workflow
- PostgreSQL data loading
- SQL queries (aggregations, joins, window functions)
- Docker containerization of Python scripts

## How to Use

1. Start the database environment:
   ```bash
   docker-compose up -d
   ```

2. Run the ingestion script for your year:
   ```bash
   python ingest_data2026.py
   ```

3. Open pgAdmin at `http://localhost:8080` to query the data

4. Follow along with the `.md` files for detailed solutions
