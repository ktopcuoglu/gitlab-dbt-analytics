import pytest
import yaml

with open(
    "../postgres_pipeline/manifests_decomposed/el_gitlab_com_db_manifest.yaml", "r"
) as db_manifest:
    el_gitlab_com_db_manifest = yaml.safe_load(db_manifest)
    implemented_tables = el_gitlab_com_db_manifest["tables"].keys()

# check are new tables are implemented
tables = ["bulk_import_entities", "geo_nodes"]

for table in tables:
    assert table in implemented_tables


# number of columns implemented for table "GEO_NODES#
assert (
    el_gitlab_com_db_manifest["tables"]["geo_nodes"]["import_query"].count(" NULL AS")
    == 21
)
