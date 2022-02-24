"""
Main test file for reduce_file_size.py
"""
import os
import pytest
from orchestration.reduce_file_size import (
    load_json_file,
    save_json_file,
    reduce_manifest_file,
    reduce_nodes_section,
    get_file_size,
)


TEST_JSON_DICT = {
    "metadata": {"adapter_type": "snowflake", "user_id": "null"},
    "test_key": "test_value",
    "nodes": {
        "test_metric": {
            "columns": {},
            "config": {"database": "PROD", "severity": "ERROR"},
            "description": "",
            "docs": {"show": "true"},
            "fqn": [
                "snowflake_spend",
                "analysis",
                "periscope_dashboards",
                "cumulative_spend_to_date",
            ],
            "meta": {},
            "name": "cumulative_spend_to_date",
            "raw_sql": "SELECT 1",
        }
    },
}
TARGET_FILE = "test_file.json"

FILE_NAME_ORIGINAL = "/Users/radbac/repos/analytics/transform/snowflake-dbt/target/manifest.json"  # TODO: rbacovic make this dynamic
FILE_NAME_REDUCED = f"{FILE_NAME_ORIGINAL}.reduced"


def clean_up_file(file_name) -> None:
    """
    Routine to delete temp file for testing
    param file_name: file name to be deleted
    return: None
    """
    if os.path.exists(file_name):
        os.remove(file_name)


def test_load_json_file_not_existing_file() -> None:
    """
    return: None
    """

    with pytest.raises(FileNotFoundError):
        _ = load_json_file("THIS_DOES_NOT_EXITS.json")


def test_load_json_file_existing_file() -> None:
    """
    return: None
    """

    save_json_file(reduced_json=TEST_JSON_DICT, target_file=TARGET_FILE)

    existing_json = load_json_file(TARGET_FILE)

    assert existing_json == TEST_JSON_DICT

    clean_up_file(TARGET_FILE)


def test_save_json_file() -> None:
    """
    return: None
    """

    save_json_file(reduced_json=TEST_JSON_DICT, target_file=TARGET_FILE)

    assert os.path.exists(TARGET_FILE)

    clean_up_file(TARGET_FILE)


def test_reduce_nodes_section() -> None:
    """
    return: None
    """

    node_json = reduce_nodes_section(source_nodes_json=TEST_JSON_DICT["nodes"].items())

    assert len(node_json["test_metric"]["config"]) == 1

    assert node_json["test_metric"]["config"]["severity"] == "ERROR"


def test_reduce_manifest_file() -> None:
    """
    return: None
    """

    reduced_json = reduce_manifest_file(raw_json=TEST_JSON_DICT)

    valid_keys = reduced_json.keys()

    config_value = reduced_json["nodes"]["test_metric"]["config"]
    assert len(config_value) == 1

    severity_value = reduced_json["nodes"]["test_metric"]["config"]["severity"]
    assert severity_value == "ERROR"

    assert "metadata" in valid_keys

    assert "nodes" in valid_keys

    assert "test_key" not in valid_keys

    assert len(valid_keys) == 2  # expect only 2 sections in dict

    assert isinstance(TEST_JSON_DICT, dict) is True

    assert isinstance(reduced_json, dict) is True


def test_get_file_size() -> None:
    """
    return: None
    """

    save_json_file(reduced_json=TEST_JSON_DICT, target_file=TARGET_FILE)

    file_size = get_file_size(TARGET_FILE)

    clean_up_file(TARGET_FILE)

    assert file_size > 0


def end_to_end_process(
    config: str, file_name_source_original: str, file_name_source_reduced: str
) -> bool:
    """
    Check end_to_end_testing, mimic the original function
    return: None
    """

    COLUMN_LIMIT_SIZE_SNOWFLAKE_MB = 14
    is_reduced = False
    config_name = config

    if os.path.exists(file_name_source_reduced):
        os.remove(file_name_source_reduced)

    if os.path.exists(file_name_source_original):
        if (
            config_name == "manifest_reduce"
            and get_file_size(file_to_measure=file_name_source_original)
            >= COLUMN_LIMIT_SIZE_SNOWFLAKE_MB
        ):

            raw_json = load_json_file(source_file=file_name_source_original)
            reduced_json = reduce_manifest_file(raw_json=raw_json)
            save_json_file(
                reduced_json=reduced_json, target_file=file_name_source_reduced
            )

            is_reduced = True
    else:
        print(
            f"Dbt File {file_name_source_original} is missing. Check if dbt run completed successfully"
        )

    return is_reduced


def test_regression_config_manifest() -> None:

    config = "manifest"
    is_reduced = end_to_end_process(
        config=config,
        file_name_source_original=FILE_NAME_ORIGINAL,
        file_name_source_reduced=FILE_NAME_REDUCED,
    )

    assert is_reduced is False
    assert os.path.exists(FILE_NAME_REDUCED) is False


def test_config_manifest_reduce() -> None:

    file_size_before = get_file_size(file_to_measure=FILE_NAME_ORIGINAL)

    config = "manifest_reduce"
    is_reduced = end_to_end_process(
        config=config,
        file_name_source_original=FILE_NAME_ORIGINAL,
        file_name_source_reduced=FILE_NAME_REDUCED,
    )

    file_size_after = get_file_size(file_to_measure=FILE_NAME_REDUCED)

    assert is_reduced is True
    assert os.path.exists(FILE_NAME_REDUCED) is True
    assert file_size_before > file_size_after


def main() -> None:
    """
    Main routine to run test cases
    """

    test_load_json_file_not_existing_file()
    test_load_json_file_existing_file()
    test_save_json_file()
    test_reduce_nodes_section()
    test_reduce_manifest_file()
    test_get_file_size()
    test_regression_config_manifest()
    test_config_manifest_reduce()


if __name__ == "__main__":
    main()
