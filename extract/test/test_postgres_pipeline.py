import pytest
import yaml

with open(
    "../postgres_pipeline/manifests_decomposed/el_gitlab_com_db_manifest.yaml", "r"
) as db_manifest:
    el_gitlab_com_db_manifest = yaml.safe_load(db_manifest)
    implemented_tables = el_gitlab_com_db_manifest["tables"].keys()

# check are new tables are implemented
table = "bulk_import_entities"
assert table in implemented_tables


with open(
    "../postgres_pipeline/manifests_decomposed/el_gitlab_com_trusted_data_extract_load_db_manifest.yaml",
    "r",
) as db_trusted_data:
    el_gitlab_com_db_trusted = yaml.safe_load(db_trusted_data)


assert table in el_gitlab_com_db_trusted["tables"]["cluster_1"]["import_query"]
