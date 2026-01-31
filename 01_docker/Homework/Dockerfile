FROM python:3.9-slim

RUN pip install --no-cache-dir pandas pyarrow sqlalchemy psycopg2-binary requests click tqdm

WORKDIR /app 
COPY ingest_data2026.py .

ENTRYPOINT ["python", "ingest_data2026.py"]