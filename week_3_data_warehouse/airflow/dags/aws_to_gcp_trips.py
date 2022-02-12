import os
from datetime import datetime, timedelta
from xml.dom.expatbuilder import FILTER_ACCEPT
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from settings import AirflowConfig, GCPConfig, AWSConfig
from functions import (
    csv_to_parquet,
    s3file_to_csv,
    upload_to_gcs
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

params = {
    "dataset": Param(
        default="green_tripdata",
        type="string",
        description="Root directory in S3 bucket to be processed"
    )
}

DATE_STRING = "{{ execution_date.strftime('%Y-%m') }}"


with DAG(
    dag_id="trips_to_gcp",
    default_args=default_args,
    schedule_interval="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 1),
    catchup=True,
    max_active_runs=2,
    params=params,
    tags=["aws", "gcp", "transfer", "trips"]
) as dag:

    FILE_PATTERN = "_".join(["{{ params.dataset }}", DATE_STRING])

    params = {
        "s3_bucket_name": AWSConfig.bucket,
        "gcp_bucket_name": GCPConfig.gcp_gcs_bucket
    }
    params["s3_file_path"] = s3_file = f"trip data/{FILE_PATTERN}.csv"
    params["csv_file"] = os.path.join(AirflowConfig.airflow_tmp, s3_file)
    params["parquet_file"] = params["csv_file"].replace(".csv", ".parquet")
    params["upload_file"] = params["parquet_file"]
    params["gcp_object_name"] = params["parquet_file"].replace(
        AirflowConfig.airflow_tmp,  "raw"
    )

    PythonOperator(
        task_id="s3_to_csv_trips",
        python_callable=s3file_to_csv,
        op_kwargs=params
    ) >> \
    PythonOperator(
        task_id="csv_to_parquet_trips",
        python_callable=csv_to_parquet,
        provide_context=True,
        op_kwargs=params
    ) >> \
    PythonOperator(
        task_id="upload_to_gcs_trips",
        python_callable=upload_to_gcs,
        provide_context=True,
        execution_timeout=timedelta(seconds=300),
        op_kwargs=params
    )

# By default, the DAG above covers the homework of doing the "green_tripdata" processing Questions 1 & 2 ... so far so good. 
# Instead of creating a new - actually equivalent - DAG for the other homework of processing the "FHV" data,  I decided to put the dataset as a parameter and use Airflow UI to configurate this parameter. Choosing dataset = "fhv_tripdata" then does the job associated with Question 3.