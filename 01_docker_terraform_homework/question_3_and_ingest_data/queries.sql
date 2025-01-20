select case when trip_distance <= 1 then '1. Up to 1 mile'
			when trip_distance > 1 and trip_distance <= 3 then '2. In between 1 (exclusive) and 3 miles (inclusive)'
			when trip_distance > 3 and trip_distance <= 7 then '3. In between 3 (exclusive) and 7 miles (inclusive)'
			when trip_distance > 7 and trip_distance <= 10 then '4. In between 7 (exclusive) and 10 miles (inclusive)'
			when trip_distance > 10 then '5. Over 10 miles'
			end as distance
, count(*) as trips
from green_taxi_tripdata
group by 1
order by 1