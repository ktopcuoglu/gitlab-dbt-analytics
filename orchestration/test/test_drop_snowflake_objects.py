from orchestration.drop_snowflake_objects import get_list_of_dbs_to_keep


def test_dbs_to_keep():
    dbs_to_keep = get_list_of_dbs_to_keep(yaml_path="load/snowflake/roles.yml")
    assert "prep" in dbs_to_keep
    assert "prod" in dbs_to_keep
    assert "raw" in dbs_to_keep
