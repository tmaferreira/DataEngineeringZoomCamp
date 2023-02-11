## Week 3 Homework - Resolution

Create External Table - Google Cloud BigQuery:

```
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp-375623.fhv_taxi_2019.external_fhv_2019`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_dezoomcamp-375623/data/fhv_ny_taxi_2019/fhv_tripdata_2019-*.csv.gz']
);
```

## Question 1:
What is the count for fhv vehicle records for year 2019?

```
SELECT count(*) 
FROM `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```
A: 43,244,696

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```
SELECT count(DISTINCT(Affiliated_base_number))
FROM `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019`;
```

A: 0 MB for the External Table and 317.94MB for the BQ Table

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```
SELECT count(*) 
FROM `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```
A: 717,748

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

A: Partition by pickup_datetime Cluster on affiliated_base_number

References:
- https://cloud.google.com/bigquery/docs/partitioned-tables
- https://cloud.google.com/bigquery/docs/clustered-tables


## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

#### Query executed in BQ table (without partitions or clusters)
```
SELECT DISTINCT(Affiliated_base_number)
FROM `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-03' AND '2019-03-31';
```

#### Partitioned Table
```
CREATE OR REPLACE TABLE `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019_partitioned`
PARTITION BY DATE(pickup_datetime) AS
SELECT * FROM `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019`;
```

#### Partitioned and Clustered Table
```
CREATE OR REPLACE TABLE `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019`;
```

#### Query executed in Partitioned and Clustered Table
```
SELECT DISTINCT(Affiliated_base_number)
FROM `dezoomcamp-375623.fhv_taxi_2019.table_fhv_2019_partitioned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
```

A: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

## Question 6: 
Where is the data stored in the External Table you created?

A: Big Query

Reference:
- https://cloud.google.com/bigquery/docs/external-tables

## Question 7:
It is best practice in Big Query to always cluster your data:

A: False

Reference:
- https://cloud.google.com/bigquery/docs/clustered-tables
