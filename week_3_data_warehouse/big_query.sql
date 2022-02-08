-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `river-howl-338521.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_river-howl-338521/raw/trip data/yellow_tripdata_*.parquet']
);

-- Check yello trip data
SELECT * FROM `river-howl-338521.nytaxi.external_yellow_tripdata` limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `river-howl-338521.nytaxi.yellow_tripdata_non_partioned` AS
SELECT * FROM `river-howl-338521.nytaxi.external_yellow_tripdata`;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `river-howl-338521.nytaxi.yellow_tripdata_partioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `river-howl-338521.nytaxi.external_yellow_tripdata`;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM `river-howl-338521.nytaxi.yellow_tripdata_non_partioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM `river-howl-338521.nytaxi.yellow_tripdata_partioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `river-howl-338521.nytaxi.yellow_tripdata_partioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM `river-howl-338521.nytaxi.yellow_tripdata_partioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM `river-howl-338521.nytaxi.yellow_tripdata_partioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

