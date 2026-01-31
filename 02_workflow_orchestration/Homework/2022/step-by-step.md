# step-by-step

Now create another DAG - for uploading the FHV data.

We will need three steps:

- Download the data
- Parquetize it
- Upload to GCS

If you don't have a GCP account, for local ingestion you'll need two steps:

- Download the data
- Ingest to Postgres

Use the same frequency and the start date as for the yellow taxi dataset

Question: how many DAG runs are green for data in 2019 after finishing everything?

Note: when processing the data for 2020-01 you probably will get an error. It's up
to you to decide what to do with it - for Week 3 homework we won't need 2020 data.

[FHV_ingestion.py](step-by-step/FHV_ingestion%20py.md)