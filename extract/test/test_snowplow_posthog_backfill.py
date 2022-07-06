"""
Test unit to check routines for SNOWPLOW -> POSTHOG historical backfilling
"""

import os
import sys

import pytest


# Tweak path as due to script
# execution way in Airflow,
# can't touch the original code

TEST_PATH = "/extract/snowplow_posthog"

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = abs_path[: abs_path.find("extract")] + TEST_PATH
sys.path.append(abs_path)


from extract.snowplow_posthog.backfill import (
    load_manifest_file,
    source_file_get_row,
    get_date_range,
    s3_get_folders,
    get_properties,
    get_property_keys,
)

MANIFEST_FILE = "../snowplow_posthog/backfill_schema.yml"

TEST_DATE_RANGE = [
    ("20220505", 24),
    ("20200606", 24),
    ("202201", 31 * 24),
    ("202202", 28 * 24),
    ("202203", 31 * 24),
    ("202204", 30 * 24),
    ("2022", 365 * 24),
    ("2020", 366 * 24),
]


def test_load_manifest_file() -> None:
    """
    test load_manifest_file

    """
    manifest_file = load_manifest_file(MANIFEST_FILE)

    property_list = manifest_file.get("gitlab_events")

    assert manifest_file is not None

    assert property_list is not None

    assert isinstance(manifest_file, dict)

    assert isinstance(property_list, list)

    assert len(property_list) >= 131


@pytest.mark.parametrize(
    "test_value, expected_length",
    [
        (
            'gitlab	srv	2022-06-06 04:00:07.042	2022-06-06 04:00:06.818	2022-06-06 04:00:06.731	struct	050a662b-79f6-4d15-a22d-61c7a85e25c0		gl	rb-0.6.1	ssc-0.15.0-kinesis	stream-enrich-0.21.0-common-0.37.0		34.74.226.8				9d220501-02bf-4661-830e-8cd487896c7d	US	SC	North Charleston	29415	32.8608	-79.9746	South Carolina																												{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.gitlab/gitlab_standard/jsonschema/1-0-8","data":{"environment":"production","source":"gitlab-rails","plan":"free","extra":{},"user_id":"448acc11d3ce44ac3434b0b2b615bfaa463fbc0b0e8f662b4d21169dc80b17c2","namespace_id":109664,"project_id":36386061,"context_generated_at":"2022-06-06 04:00:06 UTC"}}]}	EventCreateService	action_active_users_project_repo																							Ruby	Unknown	Unknown		unknown	OTHER															Unknown	Unknown	Other		Unknown	0													America/New_York				2022-06-06 04:00:06.733					2022-06-06 04:00:06.816	com.google.analytics	event	jsonschema	1-0-0',
            129,
        ),
        (
            'gitlab	srv	2022-06-06 04:00:07.042	2022-06-06 04:00:06.818	2022-06-06 04:00:06.731	struct	050a662b-79f6-4d15-a22d-61c7a85e25c0		gl	rb-0.6.1	ssc-0.15.0-kinesis	stream-enrich-0.21.0-common-0.37.0		34.74.226.8				9d220501-02bf-4661-830e-8cd487896c7d	US	SC	North Charleston	29415	32.8608	-79.9746	South Carolina																												{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.gitlab/gitlab_standard/jsonschema/1-0-8","data":{"environment":"production","source":"gitlab-rails","plan":"free","extra":{},"user_id":"448acc11d3ce44ac3434b0b2b615bfaa463fbc0b0e8f662b4d21169dc80b17c2","namespace_id":109664,"project_id":36386061,"context_generated_at":"2022-06-06 04:00:06 UTC"}}]}	EventCreateService	action_active_users_project_repo																							Ruby	Unknown	Unknown		unknown	OTHER															Unknown	Unknown	Other		Unknown	0													America/New_York				2022-06-06 04:00:06.733					2022-06-06 04:00:06.816	com.google.analytics	event	jsonschema	1-0-0	\n',
            129,
        ),
        ("", 1),
        (None, 0),
        ("This	is	test", 3),
        ("This	is	test	\n", 3),
    ],
)
def test_source_file_get_row(test_value, expected_length):
    """
    test source_file_get_row
    """
    result = source_file_get_row(test_value)

    assert isinstance(result, list)

    if test_value is not None:
        assert "\n" not in result[-1]
    else:
        assert result == []

    assert len(result) == expected_length


@pytest.mark.parametrize(
    "test_value, expected_length",
    TEST_DATE_RANGE,
)
def test_get_date_range(test_value, expected_length):
    """
    test_get_date_range
    """

    result = get_date_range(test_value)

    assert len(result) == expected_length
    assert isinstance(result, list)

    # check first hour in the month
    assert "00:00:00" in str(result[0])

    # check last hour in the month
    assert "23:00:00" in str(result[-1])

    # check month for first day
    assert test_value[4:6] in str(result[0])

    # check month for last day
    assert test_value[4:6] in str(result[-1])

    # check year
    assert test_value[:4] in str(result[-1])

    # Checks applicable for YYYY and YYYYMM
    if len(test_value) < 7:
        # check first day of month
        assert "-01" in str(result[0])

        # check last day of month
        assert (
            int(str(result[-1])[8:11]) >= 28
        )  # the last day should be at least 28 days in a month


@pytest.mark.parametrize(
    "test_value, expected_length",
    TEST_DATE_RANGE,
)
def test_get_file_prefix(test_value, expected_length):
    """
    test get_file_prefix()
    """

    result = s3_get_folders(test_value)

    assert len(result) == expected_length

    assert isinstance(result, list)

    for res in result:
        assert "output/" in res


def test_get_property_keys():
    """
    test get_property_keys
    """
    result = get_property_keys(MANIFEST_FILE, "gitlab_events")

    assert isinstance(result, list)
    assert len(result) == 131

    result = get_property_keys(MANIFEST_FILE, "non_existing_key")

    assert result == []

    with pytest.raises(FileNotFoundError):
        _ = get_property_keys("NON_EXISTING_FILE", "gitlab_events")


def test_get_properties():
    """
    test get_properties
    """
    property_list = ["a", "b", "c", "d"]
    values = [1, 2, 3]
    result = get_properties(property_list=property_list, values=values)

    assert isinstance(result, dict)
    assert property_list == list(result.keys())
