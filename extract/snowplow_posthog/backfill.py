"""
Main routine for SNOWPLOW -> POSTHOG historical backfilling
For this task will use PostHog Python API library.
Library URL: https://posthog.com/docs/integrate/server/python
"""

import yaml
import sys
from os import environ as env

from fire import Fire
from logging import info, basicConfig

import boto3
import gzip

ENCODING = "utf-8"


"""
Extract routines
"""


def get_s3_credentials() -> tuple:
    """
    This function returns the set of aws_access_key_id,aws_secret_access_key
    based on the the schema name provided.
    """

    aws_access_key_id = env["POSTHOG_ACCESS_KEY_ID"]
    aws_secret_access_key = env["POSTHOG_SECRET_ACCESS_KEY"]
    aws_s3_posthog_bucket = env["POSTHOG_S3_BUCKET"]

    return aws_access_key_id, aws_secret_access_key, aws_s3_posthog_bucket


def s3_list_files(client, bucket, prefix="") -> str:
    """
    List files in specific S3 URL using yield
    """
    paginator = client.get_paginator("list_objects")
    for result in client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents"):
        yield result["Key"]


def s3_get_client(
    aws_access_key_id: str, aws_secret_access_key: str
) -> boto3.resources.base.ServiceResource:
    """
    Get s3 client with the file list
    """

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
    )
    return session.client("s3")


def s3_load_source_file(client, bucket: str, file_name: str) -> list:
    """
    Load data from object storage (for now, it is a S3 bucket)
    """
    csv_obj = client.get_object(Bucket=bucket, Key=file_name)

    body = csv_obj["Body"]

    with gzip.open(body, "rt") as gz_file:
        for line in gz_file.readlines():
            yield line.split("\t")


def s3_extraction(file_prefix: str) -> None:

    """
    Load data from tsv files stored in an S3 Bucket and push it to PostHog.
    Loader will iterate through all files in the provided bucket that have the `.tsv`/`.gz` extension.

    """
    aws_access_key_id, aws_secret_access_key, aws_s3_posthog_bucket = get_s3_credentials

    s3_client = s3_get_client(aws_access_key_id, aws_secret_access_key)

    posthog_file_list = s3_list_files(s3_client, aws_s3_posthog_bucket, prefix="")
    # for file in s3_list_files(
    #     client=s3_client, bucket=aws_s3_posthog_bucket, prefix=file_prefix
    # ):
        pass


"""
Load routines
"""


def get_row() -> list:
    """
    return list of values for one row from the file
    """
    pass


def load_manifest_file(file_name: str) -> dict:
    """
    Load manifest file with schema definition
    """
    with open(file_name, "r", encoding=ENCODING) as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.FullLoader)


def get_properties(schema_file: str, table_name: str, values: list) -> dict:
    """
    Get key-value pairs for properties for uploading
    """
    properties_list = load_manifest_file(file_name=schema_file).get(table_name)

    return dict(zip(properties_list, values))


def get_upload_structure(values: list) -> dict:
    """
    return the prepared structure for the upload.
    Example of payload JSON to push to PostHog:
    {
        "event": "[event name]",
        "distinct_id": "[your users' distinct id]",
        "properties": {
            "key1": "value1",
            "key2": "value2"
        },
        "timestamp": "[optional timestamp in ISO 8601 format]"
    }

    """

    api_skeleton = {
        "event": "[event name]",
        "distinct_id": "[your users' distinct id]",
        "properties": {"key1": "value1", "key2": "value2"},
        "timestamp": "[optional timestamp in ISO 8601 format]",
    }

    return api_skeleton


def push_row_to_posthog(row: dict) -> None:
    """
    use PostHog lib to push
    historical record to PostHog as a part of BackFill process
    """

    pass


def s3_posthog_push(month: str) -> None:
    # get the data from S3 bucket
    s3_extraction(file_prefix=month)

    # transform data from .tsv -> .json

    # push data to PostHog


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)

    Fire(
        {
            "s3_posthog_push": s3_posthog_push,
        }
    )
    info("Upload complete.")
