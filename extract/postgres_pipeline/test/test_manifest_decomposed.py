import pytest
import yaml
from yaml.loader import SafeLoader

TABLES_LIST = [
    "path_locks",
    "lfs_file_locks",
    "bulk_import_entities",
    "clusters_integration_prometheus",
    "group_import_states",
]


def load_yaml_file(file_name: str) -> dict:
    with open(file=file_name) as f:
        data = yaml.load(f, Loader=SafeLoader)
    return data


def test_remove_incremental_tables():
    loaded_file = load_yaml_file(
        "../manifests_decomposed/el_gitlab_com_db_manifest.yaml"
    )
    for table in TABLES_LIST:
        assert loaded_file.get("tables").get(table, None) is None


def test_add_scd_tables():
    loaded_file = load_yaml_file(
        "../manifests_decomposed/el_gitlab_com_scd_db_manifest.yaml"
    )

    for table in TABLES_LIST:
        table_definition = loaded_file.get("tables").get(table, None)
        assert table_definition is not None
        assert table_definition.get("export_table", None) == table


if __name__ == "__main__":
    test_remove_incremental_tables()
    test_add_scd_tables()
