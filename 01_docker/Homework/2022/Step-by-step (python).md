# Step-by-step (python)

Create `docker-compose.yaml` 
```python
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
```

```python
docker compose up
```

access the pgcli

```python
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

![image.png](Step-by-step%20(python)/image%203.png)

Currently we have no table.

Now we make the `ingest_data.py` 

[Previous code ingest_data.py](Step-by-step%20(python)/Previous%20code%20ingest_data%20py.md)

[ingest_data2022.py](ingest_data2022%20py.md) 

```python
python ingest_data.py \
	--user=root \
	--password=root \
	--host=localhost \
	--port=5432 \
	--db=ny_taxi 
```