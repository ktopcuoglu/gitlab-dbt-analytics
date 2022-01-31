"""
Main test file for reduce_file_size.py
"""
import pytest
import os
from orchestration.reduce_file_size import (
    load_json_file,
    save_json_file,
    reduce_manifest_file,
    reduce_nodes_section,
)

test_json_dict = {
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
target_file = "test_file.json"


def clean_up_file(file_name):
    if os.path.exists(file_name):
        os.remove(file_name)


def test_load_json_file_not_existing_file():
    with pytest.raises(FileNotFoundError):
        non_existing_json = load_json_file("THIS_DOES_NOT_EXITS.json")


def test_load_json_file_existing_file():

    save_json_file(reduced_json=test_json_dict, target_file=target_file)

    existing_json = load_json_file(target_file)

    assert existing_json == test_json_dict

    clean_up_file(target_file)


def test_save_json_file():

    save_json_file(reduced_json=test_json_dict, target_file=target_file)

    assert os.path.exists(target_file)

    clean_up_file(target_file)


def test_reduce_nodes_section():
    node_json = reduce_nodes_section(source_nodes_json=test_json_dict["nodes"].items())

    assert len(node_json["test_metric"]["config"]) == 1

    assert node_json["test_metric"]["config"]["severity"] == "ERROR"


def test_reduce_manifest_file():
    reduced_json = reduce_manifest_file(raw_json=test_json_dict)

    valid_keys = reduced_json.keys()

    assert len(reduced_json["nodes"]["test_metric"]["config"]) == 1

    assert reduced_json["nodes"]["test_metric"]["config"]["severity"] == "ERROR"

    assert "metadata" in valid_keys

    assert "nodes" in valid_keys

    assert "test_key" not in valid_keys

    assert len(valid_keys) == 2 # expect only 2 sections in dict


def main():
    test_load_json_file_not_existing_file()
    test_load_json_file_existing_file()
    test_save_json_file()
    test_reduce_nodes_section()
    test_reduce_manifest_file()


if __name__ == "__main__":
    main()
