import pandas as pd  ## pandas is a library for data manipulation and analysis
import pyarrow.parquet as pq  ## pyarrow is a library for reading and writing Parquet files
import pyarrow.csv as pc  ## pyarrow.csv is a module for reading and writing CSV files
import argparse  ## for parsing command-line arguments
from time import time ## for measuring time intervals
from sqlalchemy import create_engine ## for connecting to the database
import requests  ## for making HTTP requests
from pathlib import Path  ## for filesystem paths
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main(user, password, host, port, db):
		## Connect to the Postgres database
		engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

		tables = [
			{
				"url": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-09.parquet",
				"table_name": "green_tripdata_2019_09",
				"datetime_cols": ["lpep_pickup_datetime", "lpep_dropoff_datetime"]
				
			},
			{
				"url": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
				"table_name": "taxi_zone_lookup",
				"datetime_cols": None
			}
		]

		for t in tables:
			process_file(t, engine)
		

def process_file(config, engine):
		url = config["url"] ## URL of the file to download
		table_name = config["table_name"] ## Name of the table to store data
		datetime_cols = config["datetime_cols"] ## List of datetime columns to convert

		filename = Path(url).name.replace("-","_")  ## Extract filename from URL
		download_file(url, filename) 

		## Convert parquet to csv if needed
		if filename.endswith(".parquet"):
			csv_filename = filename.replace(".parquet", ".csv")
			table = pq.read_table(filename)
			pc.write_csv(table, csv_filename)
		else:
			csv_filename = filename

		ingest_data_to_postgres(csv_filename, table_name, datetime_cols, engine)


def download_file(url, filename): ## function to download a file from a URL
		response = requests.get(url) ## make a GET request to download the file
		response.raise_for_status() ## raise an error for bad responses
		logging.info(f"Downloading {filename}...")
		with open(filename, "wb") as f: ## write the content to a file
				f.write(response.content)
		logging.info(f"Downloaded {filename} successfully.")

def ingest_data_to_postgres(csv_filename, table_name, datetime_cols, engine, chunksize=100000):
		logging.info(f"Starting ingestion of {csv_filename} into table {table_name}...")
		#Read data in chunks
		df_iter = pd.read_csv(csv_filename,iterator=True, chunksize=chunksize)

		df = next(df_iter) ## pulls the first chunk of data

		if datetime_cols:
			for col in datetime_cols:
				df[col] = pd.to_datetime(df[col]) ## fix datetime columns
#		df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime) ## fix datetime columns
#		df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

		df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False) ## create empty table based on schema of dataframe
		logging.info(f"Created table {table_name} in the database.")
		df.to_sql(name=table_name, con=engine, if_exists='append', index=False) ## insert first chunk
		logging.info(f"Inserted first chunk into {table_name}.")

		for df in df_iter:  ## iterate over remaining chunks
			t_start = time()
			if datetime_cols:
				for col in datetime_cols:
					df[col] = pd.to_datetime(df[col]) ## fix datetime columns
			
			df.to_sql(name=table_name, con=engine, if_exists='append', index=False) ## insert chunk
			t_end = time()
			logging.info('Inserted another chunk into %s, took %.3f second', table_name, (t_end - t_start))

#		while True:
#			t_start = time()
#			df = next(df_iter)
#			df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#			df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
#
#			df.to_sql(name=table_name, con=engine, if_exists='append')
#
#			t_end = time()
#
#			print('inserted another chunk...., took %.3f second' % (t_end - t_start))

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

	# user, password, host, port, database name, table name, url of the csv
	parser.add_argument('--user', help='user name for postgres')
	parser.add_argument('--password', help='password for postgres')
	parser.add_argument('--host', help='host for postgres')
	parser.add_argument('--port', help='port for postgres')
	parser.add_argument('--db', help='database name for postgres')

	args = parser.parse_args()
	main(
		user=args.user,
		password=args.password,
		host=args.host,
		port=args.port,
		db=args.db
	)