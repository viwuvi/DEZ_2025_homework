FROM python:3.12.7

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow fastparquet requests

WORKDIR /app
COPY ingest_data.py ingest_data.py

RUN wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
RUN gunzip green_tripdata_2019-10.csv.gz
RUN wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

ENTRYPOINT ["python", "ingest_data.py"]
