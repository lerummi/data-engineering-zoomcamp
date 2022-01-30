from airflow import DAG
from airflow.operators.python import PythonOperator

from settings import AirflowConfig, GCPConfig
from io import (
    csv_to_parquet, 
    filter_bucket_files, 
    s3file_to_csv, 
    upload_to_gcs
)

bucket = "nyc-tlc"
files = filter_bucket_files(bucket , "yellow_tripdata(.*)20(19|20)")

default_args = {
    "owner": "airflow",
    "start_date": None,
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="aws_to_gcp_transfer",
    schedule_interval=None, # Only triggered manually
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["aws", "gcp", "transfer"],
) as dag:

    for s3_file in files:

        parquet_file = s3_file.replace(".csv", ".parquet")

        PythonOperator(
            task_id="s3_to_csv",
            python_callable=s3file_to_csv,
            op_kwargs={
                "bucket": GCPConfig.gcp_gcs_bucket,
                "s3_file_path": s3_file, 
                "output_dir": AirflowConfig.airflow_home}
        ) >> \
        PythonOperator(
            task_id="csv_to_parquet",
            python_callable=csv_to_parquet,
            op_kwargs={
                "src_file": f"{AirflowConfig.airflow_home}/{s3_file}",
            },
        ) >> \
        PythonOperator(
            task_id="upload_to_gcs",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": GCPConfig.gcp_gcs_bucket,
                "object_name": f"raw/{parquet_file}",
                "local_file": f"{AirflowConfig.airflow_home}/{parquet_file}",
            },
        )
