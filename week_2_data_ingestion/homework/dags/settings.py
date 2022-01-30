from google.cloud import storage
from pydantic import Field


class AirflowConfig:

    airflow_home = Field("/opt/airflow", env="AIRFLOW_HOME")

class AWSConfig:

    access_key_id = Field(..., env="AWS_ACCESS_KEY_ID")
    secret_access_key = Field(..., env="AWS_SECRET_ACCESS_KEY")
    default_region = Field("eu-central-1", env="AWS_DEFAULT_REAGION")


class GCPConfig:

    gcp_project_id = Field(..., env="GCP_PROJECT_ID")
    gcp_gcs_bucket = Field(..., env="GCP_GCS_BUCKET")

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024,  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024,  # 5 MB

    client = storage.Client()
