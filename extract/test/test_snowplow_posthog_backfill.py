"""
Test unit to check routines for SNOWPLOW -> POSTHOG historical backfilling
"""

import os
import sys

import pytest


# Tweak path as due to script
# execution way in Airflow,
# can't touch the original code

test_path = "/extract/snowplow_posthog"

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = abs_path[: abs_path.find("extract")] + test_path
sys.path.append(abs_path)

from extract.snowplow_posthog.backfill import (
    load_manifest_file,
    get_properties,
    get_upload_structure,
    get_row,
)


def test_get_row():
    """
    test
    """
    pass


def test_load_manifest_file():
    """
    test
    """
    pass


def test_get_properties():
    """
    test
    """
    pass


def test_get_upload_structure():
    """
    test
    """
    pass


if __name__ == "__main__":

    test_get_row()
    test_load_manifest_file()
    test_get_properties()
    test_get_upload_structure()
