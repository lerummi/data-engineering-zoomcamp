## Homework

[Form](https://forms.gle/ytzVYUh2RptgkvF79)  
We will use all the knowledge learned in this week. Please answer your questions via form above.  
**Deadline** for the homework is 14th Feb 2022 17:00 CET.

### Question 1:

**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(\*)

---

Answer:

_SELECT count(\*) \
FROM `river-howl-338521.trips_data_all.fhv_tripdata` \
WHERE DATE(pickup_datetime) BETWEEN "2019-01-01" AND "2019-12-31"_

_Result: 42.084.899_

---

### Question 2:

**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1

---

Answer:

_SELECT count(distinct(dispatching_base_num)) \
FROM `river-howl-338521.trips_data_all.fhv_tripdata` \
WHERE DATE(pickup_datetime) BETWEEN "2019-01-01" AND "2019-12-31"_

_Result: 792_

---

### Question 3:

**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.  
We need to think what will be the most optimal strategy to improve query
performance and reduce cost.

---

Answer:

- Partion table by column **dropoff_datetime** (daily)
- Cluster by **dispatching_base_num** because ordering is already in place

---

### Question 4:

**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a
count(\*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

---

Answer:

Partition by **pickup_datetime** and cluster by **dispatching_base_num**:

_CREATE OR REPLACE TABLE `river-howl-338521.trips_data_all.fhv_tripdata_partioned_clustered` \
PARTITION BY DATE(pickup_datetime) \
CLUSTER BY dispatching_base_num AS \
SELECT \* FROM `river-howl-338521.trips_data_all.fhv_tripdata`_

and query for count (dont use wildcard, because not necessary):

_SELECT count(\*) \
FROM `river-howl-338521.trips_data_all.fhv_tripdata_partioned_clustered` \
WHERE \
DATE(pickup_datetime) BETWEEN "2019-01-01" AND "2019-03-31" \
AND \
dispatching_base_num IN ("B00987", "B02060", "B02279")_

Result:

- Estimated 400.1 MB
- Actual 155.7 MB
- Count result: 26647

---

### Question 5:

**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video.
Partitioning cannot be created on all data types.

---

Answer:

Given a query

_SELECT \* FROM `river-howl-338521.trips_data_all.fhv_tripdata` \
WHERE SR_Flag = 2 AND dispatching_base_num = "B02627"_

- Check out types and distinct values for columns:
  - dispatching_base_num: 870 (string) -> no partitioning possible due to data type
  - SR_Flag: 43 (integer), i.e. low cardinality -> partioning possible using integer range
- Setting up experiment:
  - Partion: SR_Flag, Cluster: dispatching_base_num -> fhv_tripdata_1
  - Run query on fhv_tripdata_1:
    - Estimated: 85.8 MB
    - Actual: 19.1 MB
  - Cluster: dispatching_base_num, SR_Flag -> hfv_tripdata_2
    - Estimated: 2.1 GB
    - Actual: 71 MB

_Result: Partion by **SR_Flag** and cluster by **dispatching_base_num** clearly wins._

---

### Question 6:

**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

---

_Answer: Since there a lot of overhead to be processed, there is no improvement for tables with less than 1 GB._

### (Not required) Question 7:

**In which format does BigQuery save data**  
Review big query internals video.
