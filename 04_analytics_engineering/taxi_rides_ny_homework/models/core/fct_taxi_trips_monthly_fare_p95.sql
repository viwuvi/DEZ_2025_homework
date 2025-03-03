{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),

calcu as (
    select 
    year as trip_year,
    month as trip_month,
    service_type, 
    PERCENTILE_CONT(fare_amount, 0.97) 
       OVER (PARTITION BY service_type, year, month) AS fare_amount_p97,
    PERCENTILE_CONT(fare_amount, 0.95) 
       OVER (PARTITION BY service_type, year, month) AS fare_amount_p95,
    PERCENTILE_CONT(fare_amount, 0.90) 
       OVER (PARTITION BY service_type, year, month) AS fare_amount_p90
    from trips_data
    where fare_amount > 0
    and trip_distance > 0
    and payment_type_description in ('Cash', 'Credit Card')
)

SELECT 
  service_type,
  trip_year,
  trip_month,
  MAX(fare_amount_p97) AS fare_amount_p97,
  MAX(fare_amount_p95) AS fare_amount_p95,
  MAX(fare_amount_p90) AS fare_amount_p90
FROM calcu
GROUP BY service_type, trip_year, trip_month

