import re
import os
import boto3
import logging
from typing import List
import pyarrow.csv as pv
import pyarrow.parquet as pq

from settings import GCPConfig


def filter_bucket_files(
    bucket_name: str,
    regex_pattern: str = None) -> List[str]:
    """
    Given S3 bucket and pattern, select all files
    matching this pattern and return list of file
    paths.
    """

    client = boto3.resource("s3")
    bucket = client.Bucket(bucket_name)

    regex = re.compile(regex_pattern)

    objs = []
    for obj in bucket.objects.all():
        if regex.search(obj.key):
            objs.append(obj.key)

    return objs


def s3file_to_csv(
    s3_bucket_name: str,
    s3_file_path: str,
    csv_file: str,
    **kwargs) -> str:
    """
    Download csv from S3 and save locally.
    """

    client = boto3.resource("s3")
    bucket = client.Bucket(s3_bucket_name)

    # Split s3_file_path for creation of S3 bucket
    # sibling directory structure
    path, filename = os.path.split(csv_file)
    if not os.path.exists(path):
        logging.info(f"Creating directory {path}.")
        os.makedirs(path)

    logging.info(f"Downloading file to  {csv_file}.")
    bucket.download_file(s3_file_path, csv_file)

    return csv_file


def csv_to_parquet(
    csv_file: str, 
    parquet_file: str, 
    **kwargs) -> str:
    """
    Convert csv to parquet format.
    """

    if not csv_file.endswith('.csv'):
        raise ValueError(
            "Can only accept source files in CSV format, for "
            "the moment"
        )

    table = pv.read_csv(csv_file)

    parquet_file = csv_file.replace('.csv', '.parquet')

    logging.info(f"Writing parquet file {parquet_file}.")

    pq.write_table(table, parquet_file)

    return parquet_file


def upload_to_gcs(
    upload_file: str,
    gcp_bucket_name: str,
    gcp_object_name: str,
    **kwargs) -> str:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param upload_file: source path & file-name
    :param bucket_name: GCS bucket name
    :param object_name: target path & file-name
    :return:
    """

    client = GCPConfig.client()

    bucket = client.bucket(gcp_bucket_name)

    blob = bucket.blob(gcp_object_name)

    logging.info(f"Uploading file {upload_file}.")
    blob.upload_from_filename(upload_file)
    logging.info(f"Uploaded file to {gcp_object_name}.")

    return gcp_object_name
