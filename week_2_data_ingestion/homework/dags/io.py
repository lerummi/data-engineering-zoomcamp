import re
import os
import boto3
import logging

import pyarrow.csv as pv
import pyarrow.parquet as pq
from .settings import GCPConfig


def filter_bucket_files(
    bucket: str,
    regex_pattern: str = None):

    client = boto3.resource("s3")
    bucket = client.Bucket(bucket)

    regex = re.compile(regex_pattern)

    keys = []
    for obj in bucket.objects.all():
        if regex.search(obj.key):
            keys.append(obj.key)

    return keys


def s3file_to_csv(
    bucket: str, 
    s3_file_path: str,
    output_dir: str = "/tmp"):

    client = boto3.resource("s3")
    bucket = client.Bucket(bucket)

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    bucket.download_file(s3_file_path, f"{output_dir}/{s3_file_path}")


def upload_to_gcs(
    bucket: str,
    object_name,
    local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    client = GCPConfig.client
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def csv_to_parquet(src_file: str):

    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    table = pv.read_csv(src_file)

    pq.write_table(table, src_file.replace('.csv', '.parquet'))
