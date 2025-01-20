import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
import requests

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    file_name = params.file_name
    file_path = '/app/' + file_name

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    with engine.connect() as connection:
        chunksize = 100000  # Adjust the chunk size as needed

        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            chunk.to_sql(table_name, con=connection, if_exists='append', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results')
    parser.add_argument('--file_name', help='name of the file where we download')

    args = parser.parse_args()

    main(args)
