"""
Main routine for SNOWPLOW -> POSTHOG historical back-filling
For this task will use PostHog Python API library.
Library URL: https://posthog.com/docs/integrate/server/python
"""

import datetime
import yaml
import sys
from os import environ as env
from itertools import zip_longest

import posthog
from fire import Fire
from logging import info, basicConfig
import logging

import boto3
import gzip
from dateutil.relativedelta import *

from dateutil.tz import tzutc


ENCODING = "utf-8"
EVENT_NAME = "test_gitlab_events"
DISTINCT_ID = "gitlab_dotcom"

"""
Extract routines
"""


def s3_get_credentials() -> tuple:
    """
    This function returns the set of aws_access_key_id,aws_secret_access_key and snowplow bucket
    """

    posthog_access_key_id = env["POSTHOG_AWS_ACCESS_KEY_ID"]
    posthog_secret_access_key = env["POSTHOG_AWS_SECRET_ACCESS_KEY"]
    snowplow_s3_bucket = env["POSTHOG_AWS_S3_SNOWPLOW_BUCKET"]

    return (posthog_access_key_id, posthog_secret_access_key, snowplow_s3_bucket)


def s3_get_client(
    aws_access_key_id: str, aws_secret_access_key: str
) -> boto3.resources.base.ServiceResource:
    """
    Get and return s3 client object
    """

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
    )
    return session.client("s3")


def s3_list_files(client, bucket, prefix="") -> str:
    """
    List files in specific S3 bucket using yield for in a cost-optimized fashion
    and return the file name
    """
    # session = boto3.Session(
    #     aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
    # )
    # s3_client = session.client("s3")
    #
    # results = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents")

    results = client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents")

    for result in results:
        yield result["Key"]

    # s3_bucket = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    #
    # info(f"BUCKET: {s3_bucket}...")

    # Iterate through files and upload
    # for obj in s3_bucket["Contents"]:
    #     yield obj["Key"]


def source_file_get_row(row: str) -> list:
    """
    Convert line from the source file to a list of strings
    Input: 'xxx'  'YYY' 'zzz'
    Output ['xxx', 'YYY', 'zzz']
    """
    separator = "\t"

    if row is None:
        return []

    result = row.split(separator)

    # exclude newline character ('\n') at the end of the line
    if result[-1] == "\n":
        return result[:-1]
    return result


def s3_load_source_file(client, bucket: str, file_name: str) -> list:
    """
    Load file content from object storage (for now, it is a S3 bucket)
    and return
    """
    csv_obj = client.get_object(Bucket=bucket, Key=file_name)

    body = csv_obj["Body"]

    # Handy trick to convert binary to text file
    with gzip.open(body, "rt") as gz_file:
        for line in gz_file.readlines():
            yield source_file_get_row(line)


def get_date_range(input_date: str) -> list:
    """
    Return the date range as a list (hourly level)
    Input is string and can take values:
    - YYYY     - will get a full year range  (2022     -> 2022-01-01 - 2022-12-31)
    - YYYYMM   - will get a full month range (202201   -> 2022-01-01 - 2022-01-31)
    - YYYYMMDD - will get a date year range  (20220101 -> 2022-01-01 - 2022-01-01)
    """

    if len(input_date) == 4:  # YYYY
        time_delta = relativedelta(years=1)
        full_date = f"{input_date}0101"
    elif len(input_date) == 6:  # YYYYMM
        time_delta = relativedelta(months=1)
        full_date = f"{input_date}01"
    elif len(input_date) == 8:  # YYYYMMDD
        time_delta = relativedelta(days=1)
        full_date = f"{input_date}"

    time_start = datetime.datetime.strptime(full_date, "%Y%m%d")
    time_end = datetime.datetime.strptime(full_date, "%Y%m%d") + time_delta

    ret_list = []

    while time_start < time_end:
        ret_list.append(time_start)

        time_start += relativedelta(hours=1)

    return ret_list


def s3_get_folders(yyyymm: str) -> list:
    """
    Get the list of file prefix

    Usage: "output/YYYY/MM/DD/HH24"
    """

    folder_name = "output/"

    return [
        f"""{folder_name}{day.strftime("%Y/%m/%d/%H")}"""
        for day in get_date_range(input_date=yyyymm)
    ]


def s3_extraction(file_prefix: str) -> None:

    """
    Load data from .tsv files (even the extension is .gz) stored in an S3 Bucket and push it to PostHog.
    Loader will iterate through all files in the provided bucket that have the `.tsv`/`.gz` extension.

    ----------------------------------------------------------------------------------------------------
    The bucket structure for events in S3 is:

    BUCKET_NAME
        /output
            /YYYY
                /MM
                    /DD
                        /HH24
                            /SnowPlowEnrichedGood-2-YYYY-MM-DD-HH24-MI-SS-UUID.gz
    ----------------------------------------------------------------------------------------------------

    File example: output/2022/06/06/04/SnowPlowEnrichedGood-2-2022-06-06-04-29-38-a3034baf-2167-42a5-9633-76318f7b5b8c.gz
    """
    # (
    #     posthog_access_key_id,
    #     posthog_secret_access_key,
    #     snowplow_s3_bucket,
    # ) = s3_get_credentials

    posthog_access_key_id = env["POSTHOG_AWS_ACCESS_KEY_ID"]
    posthog_secret_access_key = env["POSTHOG_AWS_SECRET_ACCESS_KEY"]
    snowplow_s3_bucket = env["POSTHOG_AWS_S3_SNOWPLOW_BUCKET"]

    property_list = get_property_keys(
        schema_file="backfill_schema.yml", table_name="gitlab_events"
    )

    s3_client = s3_get_client(posthog_access_key_id, posthog_secret_access_key)

    folders = s3_get_folders(file_prefix)

    # get folders
    for folder in folders:

        logging.info(f"Folder: {folder}...")

        snowplow_files = s3_list_files(
            client=s3_client, bucket=snowplow_s3_bucket, prefix=folder
        )

        # get files
        for snowplow_file in snowplow_files:
            logging.info(f"     File: {snowplow_file}")

            # get row
            for row in s3_load_source_file(
                client=s3_client, bucket=snowplow_s3_bucket, file_name=snowplow_file
            ):
                json_prepared = get_properties(property_list=property_list, values=row)
                # push row to PostHog
                posthog_push_json(json_prepared)


"""
Load routines
"""


def posthog_get_credentials() -> tuple:
    """
    This function returns the set of PostHog secrets.
    """

    posthog_project_api_key = env["POSTHOG_PROJECT_API_KEY"]
    posthog_personal_api_key = env["POSTHOG_PERSONAL_API_KEY"]
    posthog_host = env["POSTHOG_HOST"]

    return (posthog_project_api_key, posthog_personal_api_key, posthog_host)


def load_manifest_file(file_name: str) -> dict:
    """
    Load manifest file with schema definition
    """
    with open(file_name, "r", encoding=ENCODING) as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.FullLoader)


def get_property_keys(schema_file: str, table_name: str) -> list:
    """
    Get list of property keys from the file
    """
    return load_manifest_file(file_name=schema_file).get(table_name, [])


def get_properties(property_list: str, values: list) -> dict:
    """
    Get key-value pairs for properties for uploading
    """
    return dict(zip_longest(property_list, values))


def posthog_authorize() -> None:
    """
    Authorize PostHog access
    """

    (
        posthog_project_api_key,
        posthog_personal_api_key,
        posthog_host,
    ) = posthog_get_credentials()

    posthog.project_api_key = posthog_project_api_key
    posthog.personal_api_key = posthog_personal_api_key
    posthog.host = posthog_host
    posthog.sync_mode = True


def posthog_push_json(data: dict) -> None:
    """
    Use PostHog lib to push
    historical record to PostHog as a part of BackFill process
    """
    posthog.capture(
        DISTINCT_ID,
        event=EVENT_NAME,
        properties=data,
        timestamp=datetime.datetime.utcnow().replace(tzinfo=tzutc()),
    )


def snowplow_posthog_backfill(day: str) -> None:
    """
    Entry point to trigger the back filling for Snowplow S3 -> PostHog
    """

    posthog_authorize()

    # get the data from S3 bucket
    s3_extraction(file_prefix=str(day))


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)

    Fire(
        {
            "snowplow_posthog_backfill": snowplow_posthog_backfill,
        }
    )
    info("Upload complete.")
