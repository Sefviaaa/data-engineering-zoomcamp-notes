# Putting Ingestion Script into Docker

create a new `Dockerfile` in the same folder

```python
FROM python:3.9-slim

RUN pip install --no-cache-dir pandas pyarrow sqlalchemy psycopg2-binary requests logging

WORKDIR /app 
COPY ingest_data.py .

ENTRYPOINT ["python", "ingest_data.py"]
```

```python
docker build -t taxi_ingest:hw2022 .
```

```python
docker run --rm \
  --network=hw-network \
  taxi_ingest:hw2022 \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi
```

The --network must match the Postgres network.
How to check your Postgres network
docker network ls
Then:
docker inspect pg-network