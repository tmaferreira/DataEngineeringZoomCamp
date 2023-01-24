-- Question 1. Knowing docker tags
-- Which tag has the following text? - Write the image ID to the file
-- > docker build --help

-- Question 2. Understanding docker first run
-- Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed (use pip list).
-- > docker run -it -entrypoint=bash python:3.9
-- >>> pip list

-- Question 3. Count records
-- How many taxi trips were totally made on January 15?
SELECT COUNT(*)
FROM public.green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-01-15' 
AND CAST(lpep_dropoff_datetime AS DATE) = '2019-01-15';

-- Question 4. Largest trip for each day
-- Which was the day with the largest trip distance? Use the pick up time for your calculations.
SELECT CAST(lpep_pickup_datetime AS DATE), trip_distance
FROM public.green_taxi_trips
GROUP BY CAST(lpep_pickup_datetime AS DATE), trip_distance
ORDER BY trip_distance DESC
LIMIT 1;

-- Question 5. The number of passengers
-- In 2019-01-01 how many trips had 2 and 3 passengers?
SELECT COUNT(*)
FROM public.green_taxi_trips
WHERE (CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' AND passenger_count = 2)
OR (CAST(lpep_pickup_datetime AS DATE) = '2019-01-01' AND passenger_count = 3)
GROUP BY CAST(lpep_pickup_datetime AS DATE), passenger_count;

-- Question 6. Largest tip
-- For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
SELECT CONCAT(zdo."Zone") AS "dropoff_zone"
FROM public.green_taxi_trips t 
JOIN public.zones zpu ON t."PULocationID" = zpu."LocationID"
JOIN public.zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE zpu."Zone" = 'Astoria'
ORDER BY tip_amount DESC
LIMIT 1;

