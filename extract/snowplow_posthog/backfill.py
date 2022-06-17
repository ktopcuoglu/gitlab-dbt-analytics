"""
Main routine for SNOWPLOW -> POSTHOG historical backfilling
For this task will use PostHog Python API library.
Library URL: https://posthog.com/docs/integrate/server/python
"""

import yaml

ENCODING = "utf-8"


def load_source_data(object_storage: str) -> None:
    """
    Load data from object storage (for now, it is a S3 bucket)
    """
    pass


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
    properties_list = \
        load_manifest_file(file_name=schema_file).get(table_name)

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
