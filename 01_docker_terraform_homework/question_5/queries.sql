select date(date_trunc('day', cast(lpep_pickup_datetime as timestamp))) as day
	, b."Zone" as pickup_zone
	, sum(total_amount) as total_amount
from green_taxi_tripdata a
left join taxi_zone_lookup b on a."PULocationID" = b."LocationID"
where date(date_trunc('day', cast(lpep_pickup_datetime as timestamp))) = '2019-10-18'
group by 1,2
having sum(total_amount) > 13000
order by 3 desc