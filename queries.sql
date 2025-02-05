
## create external table
CREATE OR REPLACE EXTERNAL TABLE `data-engineer-zoomcamp-2025.zoomcamp.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://data-engineer-zoomcamp-vivian/yellow_tripdata_2024-*.parquet']
);


## create materialized table in BQ
CREATE OR REPLACE TABLE `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024` AS
SELECT * FROM `data-engineer-zoomcamp-2025.zoomcamp.external_yellow_tripdata`;


## Question 1
select count(*)
from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024`


## Question 2
select count(distinct PULocationID)
from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024`

select count(distinct PULocationID)
from `data-engineer-zoomcamp-2025.zoomcamp.external_yellow_tripdata`


## Question 3
select count(distinct PULocationID), count(distinct DOLocationID)
from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024`


## Question 4
select count(*)
from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024`
where fare_amount = 0


## Question 5
CREATE OR REPLACE TABLE `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024_partitoned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `data-engineer-zoomcamp-2025.zoomcamp.external_yellow_tripdata`;

## Question 6
select distinct VendorID
from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024`
where DATE(tpep_dropoff_datetime) >= '2024-03-01'
and DATE(tpep_dropoff_datetime) <= '2024-03-15'

select distinct VendorID
from `data-engineer-zoomcamp-2025.zoomcamp.yellow_tripdata_2024_partitoned`
where DATE(tpep_dropoff_datetime) >= '2024-03-01'
and DATE(tpep_dropoff_datetime) <= '2024-03-15'




