## Week 4 Homework - WIP

[Form]() - To be published soon
We will use all the knowledge learned in this week. Please answer your questions via form above.

- You can submit your homework multiple times. In this case, only the last submission will be used.
  **Deadline** for the homework is 21th Feb 2022 17:00 CET.

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:

- Building a source table
- Building a fact table
- Create a dashboard

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

### Question 1:

**What is the count of records in the model fact_trips after running all models with the test run variable disabled (:false)**  
You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI.
You should find the views and models for querying in your DWH.

---

Answer:

Run Query

_SELECT count(\*) \
FROM `river-howl-338521.production.fact_trips`
WHERE pickup_datetime BETWEEN "2019-01-01" AND "2020-12-31"_

Result: In table `fact_trips` check out "Details" -> "Number of rows": 61,600,051

---

### Question 2:

**What is the distribution between service type filtering by years 2019 and 2020 data**. You will need to complete "Visualising the data" videos, either using data studio or metabase.

---

Answer: It's 89.9 % "Yellow" vs 10.1 % "Green"

### Question 3:

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  
Create a staging model for the fhv data for 2019 keeping only records with entries for affiliated_base_number.
Run it via the CLI without limits (is_test_run: false).

---

Answer:

Run Query

_SELECT count(\*) \
FROM `river-howl-338521.production.stg_fhv_tripdata`_

Result: 46,275,630

---

### Question 4:

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  
Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with knwon pickup and doproff locations entries for pickup and dropoff locations.
Run it via the CLI without limits (is_test_run: false).

---

Answer:

Run Query

_SELECT count(\*) \
FROM `river-howl-338521.production.fact_fhv_trips`_

Result: 19,126,526

### Question 5:

**TBD - Something about the dashboard**
