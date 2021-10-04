import pytest
import yaml

########################################################################################################################
# Test set up for new tables:
# - bulk_import_entities
# - compliance_management_frameworks
########################################################################################################################

test_dir = "../postgres_pipeline/manifests_decomposed/"
test_pattern = {
    "bulk_import_entities": "el_gitlab_com_db_manifest.yaml",
    "compliance_management_frameworks": "el_gitlab_com_scd_db_manifest.yaml",
}

for test_table, test_file in test_pattern.items():
    with open(f"{test_dir}{test_file}", "r") as test_file_yaml:
        test_file_manifest = yaml.safe_load(test_file_yaml)
        implemented_tables = test_file_manifest["tables"].keys()

    assert test_table in implemented_tables
    assert test_table == test_file_manifest["tables"][test_table]["export_table"]
    assert test_table in test_file_manifest["tables"][test_table]["import_query"]

########################################################################################################################
# Test set up for new trusted data framework:
# - bulk_import_entities
########################################################################################################################
test_pattern = {
    "bulk_import_entities": "el_gitlab_com_trusted_data_extract_load_db_manifest.yaml",
}

for test_table, test_file in test_pattern.items():
    with open(f"{test_dir}{test_file}", "r") as db_trusted_data:
        el_gitlab_com_db_trusted = yaml.safe_load(db_trusted_data)

    assert test_table in el_gitlab_com_db_trusted["tables"]["cluster_1"]["import_query"]
    assert (
        el_gitlab_com_db_trusted["tables"]["cluster_1"]["import_query"].count(
            test_table
        )
        == 2
    )  # see is SQL correct
