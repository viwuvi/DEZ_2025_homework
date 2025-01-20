select date(date_trunc('day', cast(lpep_pickup_datetime as timestamp))) as day
	, max(trip_distance) as longest_distances
from green_taxi_tripdata
group by 1
order by 2 desc