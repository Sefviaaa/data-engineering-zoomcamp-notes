import os
from time import time
import pandas as pd
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_file, execution_date): ## just pass parameters here without lengthening the function 
		print(table_name, csv_file, execution_date)
		t_start = time() ## start time logs

		engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
		engine.connect() ## test connection to make sure that it works
		print("Connection to the database successful, inserting data...")

		df_iter = pd.read_csv(csv_file,compression='gzip',iterator=True, chunksize=100000 )
		df = next(df_iter)

		df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
		df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

		df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace') ## create a table if it does not exist and replace if it exists

		df.to_sql(name=table_name, con=engine, if_exists='append')
		t_end = time()
		print('inserted the first chunk...., took %.3f second' % (t_end - t_start))

		while True:
			t_start = time()
			df = next(df_iter)
			df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
			df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

			df.to_sql(name=table_name, con=engine, if_exists='append')

			t_end = time()

			print('inserted another chunk...., took %.3f second' % (t_end - t_start))
