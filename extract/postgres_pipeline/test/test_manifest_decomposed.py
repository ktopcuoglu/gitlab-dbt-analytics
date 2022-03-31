"""
Testing routine for manifest decomposition
"""
import os
import sys

import pytest

import yaml
from yaml.loader import SafeLoader

# Tweak path as due to script execution way in Airflow,
# can't touch the original code
abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path = (
    abs_path[: abs_path.find("extract")]
    + "extract/postgres_pipeline/manifests_decomposed/"
)
sys.path.append(abs_path)

TABLES_LIST = [
    "path_locks",
    "lfs_file_locks",
    "bulk_import_entities",
    "clusters_integration_prometheus",
    "group_import_states",
]


def load_yaml_file(file_name: str) -> dict:
    """
    Load yaml file to ensure changes didn't break anything

    param file_name: input file name
    return: dict
    """
    with open(file=file_name, encoding="utf-8") as file:
        data = yaml.load(file, Loader=SafeLoader)
    return data


def test_remove_incremental_tables() -> None:
    """
    Check are tables from incremental removed properly
    from the el_gitlab_com_db_manifest file.
    return: None
    """
    full_path = f"{abs_path}el_gitlab_com_db_manifest.yaml"
    loaded_file = load_yaml_file(full_path)
    for table in TABLES_LIST:
        with pytest.raises(KeyError):
            assert loaded_file["tables"][table] is None


def test_add_scd_tables() -> None:
    """
    Check are tables from incremental load are
    added properly to SCD load into file
    el_gitlab_com_scd_db_manifest
    return: None
    """
    full_path = f"{abs_path}el_gitlab_com_scd_db_manifest.yaml"
    loaded_file = load_yaml_file(full_path)

    for table in TABLES_LIST:
        table_definition = loaded_file["tables"][table]
        assert table_definition is not None
        assert table_definition.get("export_table", None) == table
        assert table_definition.get("advanced_metadata", None) is True


if __name__ == "__main__":
    test_remove_incremental_tables()
    test_add_scd_tables()
