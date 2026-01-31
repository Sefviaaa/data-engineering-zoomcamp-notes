import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import pyarrow.dataset as ds
from pathlib import Path
import requests

def get_engine(pg_user, pg_pass, pg_host, pg_port, pg_db):
    """Create a SQLAlchemy engine for the Postgres database."""
    return create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

def ingest_taxi_zones(engine, table_name="taxi_zones"):
    """Ingest taxi zone lookup data into Postgres."""
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"

    df = pd.read_csv(url)

    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    print (f'Ingested {len(df)} rows into {table_name} table.')

def download_parquet_file(url: str, local_path: Path):
    """Download a parquet file from a URL to a local path."""
    if local_path.exists():
        print(f'File {local_path} already exists. Skipping download.')
        return

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024*1024):
            f.write(chunk)

@click.command()
@click.option('--pg_user', help="Postgres user", default="root")
@click.option('--pg_pass', help="Postgres password", default="root")
@click.option('--pg_host', help="Postgres host", default="localhost")
@click.option('--pg_port', help="Postgres port", default=5432)
@click.option('--pg_db', help="Postgres database name", default="ny_taxi")
@click.option('--pg_year', help="Year of the data to ingest", default=2026, type=int)
@click.option('--pg_month', help="Month of the data to ingest", default=1, type=int)
@click.option('--chunk_size', help="Number of rows per chunk", default=100000, type=int)
@click.option('--ingest_zones', is_flag=True, help="Ingest taxi zone lookup table")

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, pg_year, pg_month, chunk_size, ingest_zones):
    """Ingest data into Postgres database in chunks."""
    # Create the database engine
    engine = get_engine(pg_user, pg_pass, pg_host, pg_port, pg_db)

    if ingest_zones:
        ingest_taxi_zones(engine)

    # Define the parquet file URL
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    url = f'{prefix}green_tripdata_{pg_year}-{pg_month:02d}.parquet'

    local_file = Path(f'green_tripdata_{pg_year}-{pg_month:02d}.parquet')

    download_parquet_file(url, local_file)

    df = ds.dataset(local_file, format="parquet")

    table_name = f'green_taxi_data_{pg_year}_{pg_month:02d}'

    # Create table if not exists
    empty_df = df.to_table().slice(0, 0).to_pandas()
    empty_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

    # Append data in chunks
    for batch in tqdm(df.to_batches(batch_size=chunk_size)):
        batch_df = batch.to_pandas()
        batch_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        tqdm.write(f'Inserted another {len(batch_df)} rows into {table_name} table.')


if __name__ == "__main__":
    run()