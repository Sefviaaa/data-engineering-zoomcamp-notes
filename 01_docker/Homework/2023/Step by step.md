# Step by step

[ingest_data2023.py](Step%20by%20step/ingest_data2023%20py.md)

Change the `Dockerfile`

```sql
FROM python:3.9-slim

RUN pip install --no-cache-dir pandas pyarrow sqlalchemy psycopg2-binary requests logging

WORKDIR /app 
COPY ingest_data2023.py .

ENTRYPOINT ["python", "ingest_data2023.py"]
```

```sql
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

```sql
docker build -t taxi_ingest:hw2023 .
```

```sql
docker run --rm \
  --network=hw-network \
  taxi_ingest:hw2023 \
  --user=root \
  --password=root \
  --host=pgdatabase \
  --port=5432 \
  --db=ny_taxi
```