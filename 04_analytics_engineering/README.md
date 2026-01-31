# 04_analytics_engineering - Analytics Engineering with dbt

This module covers analytics engineering concepts and hands-on implementation using dbt (data build tool), as part of the Data Engineering Zoomcamp.

## Contents

| Folder | Description |
|--------|-------------|
| `taxi_rides_ny/` | dbt project for NYC taxi data transformations |
| `Homework/` | Homework solutions with Airflow DAGs |

## dbt Project

The `taxi_rides_ny/` folder is a complete dbt project with:

| File/Folder | Description |
|-------------|-------------|
| `dbt_project.yml` | Project configuration |
| `models/` | SQL transformation models |
| `macros/` | Reusable SQL macros |
| `seeds/` | Static CSV data |
| `tests/` | Data quality tests |

## Quick Start

### 1. Install dbt

```bash
pip install dbt-bigquery
```

### 2. Run dbt Models

```bash
cd taxi_rides_ny
dbt run
```

### 3. Test dbt Models

```bash
dbt test
```

## Topics Covered

- **dbt Fundamentals**: Models, sources, seeds, tests
- **Data Modeling**: Staging, intermediate, mart layers
- **Jinja & Macros**: Reusable SQL templating
- **Documentation**: Auto-generated data docs

## Homework

See the [Homework](./Homework) folder for homework solutions.
