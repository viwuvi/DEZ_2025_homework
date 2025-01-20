select distinct b."Zone" as pickup_zone
	, c."Zone" as dropoff_zone
	, tip_amount
from green_taxi_tripdata a
left join taxi_zone_lookup b on a."PULocationID" = b."LocationID"
left join taxi_zone_lookup c on a."DOLocationID" = c."LocationID"
where b."Zone" = 'East Harlem North'
order by 3 desc