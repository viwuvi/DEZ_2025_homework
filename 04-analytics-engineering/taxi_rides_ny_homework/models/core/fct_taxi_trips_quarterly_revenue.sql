{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),

calcu as (
    select 
    year,
    quarter,
    year_quarter as revenue_quarter, 
    service_type, 
    count(distinct tripid) as number_of_trips,
    sum(total_amount) as revenue_monthly_total_amount,
    from trips_data
    group by 1,2,3,4
)

select a.service_type
    , a.revenue_quarter
    , a.number_of_trips
    , a. revenue_monthly_total_amount as revenue_monthly_total_amount_TY
    , b. revenue_monthly_total_amount as revenue_monthly_total_amount_LY
    , safe_divide(a.revenue_monthly_total_amount,b.revenue_monthly_total_amount) -1 as YoY_growth
from calcu a
left join calcu b on a.service_type = b.service_type
                and a.quarter = b.quarter
                and a.year = b.year + 1