import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from settings import AirflowConfig, GCPConfig, AWSConfig
from functions import (
    filter_bucket_files,
    csv_to_parquet,
    s3file_to_csv,
    upload_to_gcs
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="zones_to_gcp",
    default_args=default_args,
    start_date=datetime.now(),
    schedule_interval="@once",
    catchup=True,
    max_active_runs=3,
    tags=["aws", "gcp", "transfer", "zones"]
) as dag:

    ZONE_BUCKET_FILES = filter_bucket_files(
        AWSConfig.bucket, 
        "(.*)zone(.*).csv"
    )

    params = {
        "s3_bucket_name": AWSConfig.bucket,
        "gcp_bucket_name": GCPConfig.gcp_gcs_bucket
    }

    for i, zone_file in enumerate(ZONE_BUCKET_FILES):

        s3_file = zone_file.replace(" ", "")

        params["s3_file_path"] = zone_file
        params["csv_file"] = os.path.join(AirflowConfig.airflow_tmp, s3_file)
        params["parquet_file"] = params["csv_file"].replace(".csv", ".parquet")
        params["upload_file"] = params["parquet_file"]
        params["gcp_object_name"] = params["parquet_file"].replace(
        AirflowConfig.airflow_tmp,  "raw"
        )

        PythonOperator(
            task_id=f"s3_to_csv_zones_{i}",
            python_callable=s3file_to_csv,
            op_kwargs=params
        ) >> \
        PythonOperator(
            task_id=f"csv_to_parquet_zones_{i}",
            python_callable=csv_to_parquet,
            provide_context=True,
            op_kwargs=params
        ) >> \
        PythonOperator(
            task_id=f"upload_to_gcs_zones_{i}",
            python_callable=upload_to_gcs,
            provide_context=True,
            op_kwargs=params
        )
