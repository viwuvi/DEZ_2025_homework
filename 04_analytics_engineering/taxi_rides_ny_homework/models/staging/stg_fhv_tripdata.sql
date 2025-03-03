{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_num,

    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    extract(year from pickup_datetime) as year,
    extract(quarter from pickup_datetime) as quarter,
    concat(extract(year from pickup_datetime), '-Q', extract(quarter from pickup_datetime)) as year_quarter,
    extract(month from pickup_datetime) as month,

    -- payment info
    cast(SR_Flag as string) as SR_Flag,
    cast(Affiliated_base_number as string) as Affiliated_base_number

from tripdata
-- where rn = 1


