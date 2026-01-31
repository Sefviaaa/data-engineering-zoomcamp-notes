# Homework Solutions - Module 04: Analytics Engineering

Yearly homework solutions for the Data Engineering Zoomcamp Week 4.

## Solutions by Year

| Year | Description |
|------|-------------|
| [2024](./2024.md) | dbt models, data transformations |
| [2025](./2025.md) | FHV trip data modeling with dbt |

## 2025 Resources

| File | Description |
|------|-------------|
| `CLI.md` | dbt CLI commands reference |
| `stg_fhv_tripdata sql.md` | Staging model for FHV trip data |
| `dim_fhv_trips sql.md` | Dimension model for FHV trips |
| `fct_fhv_monthly_zone_traveltime_p90 sql.md` | Fact table for travel time percentiles |

## Supporting Files

| File/Folder | Description |
|-------------|-------------|
| `Dockerfile` | Airflow container configuration |
| `docker-compose.yaml` | Airflow services setup |
| `dags/` | Airflow DAGs for data ingestion |

## Topics Covered

- Building dbt models (staging, dimensions, facts)
- Configuring dbt projects
- Running dbt via CLI
- Creating data transformations for FHV taxi data
