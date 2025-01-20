# README.md

## Ojective

### The purpose of this README file is to explain how to load data into Postgres using Docker.

## Process

### Run the yaml file to build the network and images of postgres and pgadmin

#### see docker-compose.yaml file in question_3_and ingest_data folder

```
docker-composer up
```


### Build the image to ingest the data to postgres 

#### see ingest_data.py and Dockerfile in question_3_and ingest_data folder

```
docker build -t homework_q3_data_ingest .
```


### First run to load the tripdata to postgres

```
 docker run -it \
     --network=question_3_pgdatabase_pgadmin_network_q3 \
     homework_q3_data_ingest \
         --user=root \
         --password=root \
         --host=pgdatabase \
         --port=5432 \
         --db=ny_taxi \
         --table_name=green_taxi_tripdata \
         --file_name="green_tripdata_2019-10.csv"
```


### Second run to get the zone to postgres

```
 docker run -it \
     --network=question_3_pgdatabase_pgadmin_network_q3 \
     homework_q3_data_ingest \
         --user=root \
         --password=root \
         --host=pgdatabase \
         --port=5432 \
         --db=ny_taxi \
         --table_name=taxi_zone_lookup \
         --file_name="taxi_zone_lookup.csv"
```