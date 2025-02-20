{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('dim_fhv_trips') }}
),

calcu as (
    select 
    year as trip_year,
    month as trip_month,
    service_type, 
    pickup_zone, 
    dropoff_zone,
    pickup_locationid, 
    dropoff_locationid,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from trips_data
),

final as (
    select 
    trip_year,
    trip_month,
    service_type, 
    pickup_zone, 
    dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.90) 
       OVER (PARTITION BY service_type, trip_year, trip_month, pickup_locationid, dropoff_locationid) AS trip_duration_p90
    from calcu
)

SELECT 
  service_type,
  trip_year,
  trip_month,
  pickup_zone, 
  dropoff_zone,
  MAX(trip_duration_p90) AS trip_duration_p90
FROM final
GROUP BY service_type, trip_year, trip_month, pickup_zone, dropoff_zone

