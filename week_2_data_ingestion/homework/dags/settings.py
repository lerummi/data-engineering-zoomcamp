import os
from google.cloud import storage


storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB


class AirflowConfig:

    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    airflow_tmp = os.environ.get("AIRFLOW_TMP", "/tmp")


class AWSConfig:

    access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    default_region = os.environ.get("AWS_DEFAULT_REGION", "eu-central-1")

    bucket = "nyc-tlc"


class GCPConfig:

    gcp_project_id = os.environ.get("GCP_PROJECT_ID")
    gcp_gcs_bucket = os.environ.get("GCP_GCS_BUCKET")

    @staticmethod
    def client():
        return storage.Client()
