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
from dateutil.relativedelta import relativedelta

from dateutil.tz import tzutc


ENCODING = "utf-8"
#EVENT_NAME = "test_gitlab_events_ved"
#DISTINCT_ID = "gitlab_dotcom"

"""
Extract routines
"""


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


def s3_list_files(client, **base_kwargs) -> str:
    """
    List files in specific S3 bucket using yield for in a cost-optimized fashion
    and return the file name.
    As the native function list_objects_v2 return 1000 files as per default.
    The current function is workaround to overcome this limitation,
    in case there are more than 1000 files in the folder.
    Probably not gonna happen, but want to be on the safe side.

    Input: s3 client, bucket and prefix as *base_kwargs
    Output: list of files names yielded for the performance purpose.
    """

    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)

        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token

        response = client.list_objects_v2(**list_kwargs)

        for result in response.get("Contents", []):
            yield result["Key"]  # yield file name

        if not response.get("IsTruncated"):  # At the end of the list?
            break

        continuation_token = response.get("NextContinuationToken")


def source_file_get_row(row: str) -> list:
    """
    Convert line from the source file to a list of strings
    Input: 'xxx'  'YYY' 'zzz'
    Output ['xxx', 'YYY', 'zzz']
    """
    field_separator = "\t"
    new_line_separator = "\n"

    if row is None:
        return []

    result = row.split(field_separator)

    # exclude newline character ('\n') at the end of the line
    if result[-1] == new_line_separator:
        return result[:-1]
    return result


def s3_load_source_file(client, bucket: str, file_name: str) -> list:
    """
    Load file content from object storage (for now, it is a S3 bucket)
    and return file line per line
    Input: file fetched by file_name from desired bucket
    Output: row from the file represented as a list
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

    Usage:
    Input: YYYYMM or YYYYMMDD
    Output: "output/YYYY/MM/DD/HH24" (24 list members for each day)
    """

    folder_name = "output/"

    return [
        f"""{folder_name}{day.strftime("%Y/%m/%d/%H")}"""
        for day in get_date_range(input_date=yyyymm)
    ]


def posthog_processing(file_prefix: str) -> None:

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

    posthog_access_key_id = env["POSTHOG_AWS_ACCESS_KEY_ID"]
    posthog_secret_access_key = env["POSTHOG_AWS_SECRET_ACCESS_KEY"]
    snowplow_s3_bucket = env["POSTHOG_AWS_S3_SNOWPLOW_BUCKET"]

    # reduce the noise in log file
    logging.getLogger(
        "botocore.vendored.requests.packages.urllib3.connectionpool"
    ).setLevel(logging.WARNING)

    property_list = get_property_keys(
        schema_file="backfill_schema.yml", table_name="gitlab_events"
    )

    s3_client = s3_get_client(posthog_access_key_id, posthog_secret_access_key)

    folders = s3_get_folders(file_prefix)

    # get folders
    for folder in folders:

        logging.info(f"Folder: {folder}...")

        snowplow_files = s3_list_files(
            client=s3_client, Bucket=snowplow_s3_bucket, Prefix=folder
        )

        # get files
        for snowplow_file in snowplow_files:
            logging.info(f"     File: {snowplow_file}")

            # get rows
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
    Input: ['a', 'b', 'c'] and [1, 2, 3]
    Output: {'a': 1, 'b': 2 , 'c': 3}
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
    Change log Ved
    Removed the timestamp to the collector_tstamp (	Timestamp for the event recorded by the collector) because when this event was stored in the posthog it was showing that it was generated now not as back date event. 
    Moved event from hard coded value of EVENT_NAME = "test_gitlab_events_ved"   to value coming in each event.  
    DISTINCT ID is set as user_ipaddress.
    These 3 were suggested by PostHog team. 
    """
    DISTINCT_ID=data["user_ipaddress"]
    posthog.capture(
        DISTINCT_ID,
        event=data["event"],
        properties=data,
        timestamp=datetime.datetime.fromisoformat(data["collector_tstamp"]) #datetime.datetime.utcnow().replace(tzinfo=tzutc()),
    )


def snowplow_posthog_backfill(day: str) -> None:
    """
    Entry point to trigger the back filling for Snowplow S3 -> PostHog
    """

    posthog_authorize()

    # process data
    posthog_processing(file_prefix=str(day))


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)

    Fire(
        {
            "snowplow_posthog_backfill": snowplow_posthog_backfill,
        }
    )
    info("Upload complete.")
