"""
Test unit to ensure quality of transformation algorithm
from Postgres to Snowflake
"""
from typing import Dict, List, Any
import re
import pytest
import sqlparse.sql

from extract.saas_usage_ping.transform_instance_level_queries_to_snowsql import (
    main,
    optimize_token_size,
    find_keyword_index,
    prepare_sql_statement,
    translate_postgres_snowflake_count,
    keep_meta_data,
    META_API_COLUMNS,
    TRANSFORMED_INSTANCE_QUERIES_FILE,
    META_DATA_INSTANCE_QUERIES_FILE,
    HAVING_CLAUSE_PATTERN,
    METRICS_EXCEPTION,
)


def test_transforming_queries():
    """
    Test case: for transforming queries from Postgres to Snowflake
    """
    test_cases_dict: Dict[Any, Any] = {
        "counts": {
            "boards": 'SELECT COUNT("boards"."id") FROM "boards"',
            "clusters_applications_cert_managers": 'SELECT COUNT(DISTINCT "clusters_applications_cert_managers"."clusters.user_id") '
            'FROM "clusters_applications_cert_managers" '
            'INNER JOIN "clusters" '
            'ON "clusters"."id" = "clusters_applications_cert_managers"."cluster_id" '
            'WHERE "clusters_applications_cert_managers"."status" IN (11, 3, 5)',
            "clusters_platforms_eks": 'SELECT COUNT("clusters"."id") '
            'FROM "clusters" '
            'INNER JOIN "cluster_providers_aws" '
            'ON "cluster_providers_aws"."cluster_id" = "clusters"."id" '
            'WHERE "clusters"."provider_type" = 2 '
            'AND ("cluster_providers_aws"."status" IN (3)) '
            'AND "clusters"."enabled" = TRUE',
            "clusters_platforms_gke": 'SELECT COUNT("clusters"."id") '
            'FROM "clusters" '
            'INNER JOIN "cluster_providers_gcp" '
            'ON "cluster_providers_gcp"."cluster_id" = "clusters"."id" '
            'WHERE "clusters"."provider_type" = 1 '
            'AND ("cluster_providers_gcp"."status" IN (3)) '
            'AND "clusters"."enabled" = TRUE',
            "clusters_platforms_user": 'SELECT COUNT("clusters"."id") '
            'FROM "clusters" '
            'WHERE "clusters"."provider_type" = 0 '
            'AND "clusters"."enabled" = TRUE',
            "incident_labeled_issues": 'SELECT COUNT("issues"."id") '
            'FROM "issues" '
            'INNER JOIN "label_links" '
            'ON "label_links"."target_type" = \'Issue\' '
            'AND "label_links"."target_id" = "issues"."id" '
            'INNER JOIN "labels" ON "labels"."id" = "label_links"."label_id" '
            'WHERE "labels"."title" = \'incident\' '
            'AND "labels"."color" = \'#CC0033\' '
            'AND "labels"."description" = \'Denotes a disruption'
            " to IT services and the associated"
            " issues require immediate attention'",
        }
    }

    results_dict: Dict[Any, Any] = {
        "counts.boards": "SELECT 'counts.boards' AS counter_name,  "
        "COUNT(boards.id) AS counter_value, "
        "TO_DATE(CURRENT_DATE) AS run_day   "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_boards_dedupe_source AS boards",
        "counts.clusters_applications_cert_managers": "SELECT 'counts.clusters_applications_cert_managers' AS counter_name,  "
        "COUNT(DISTINCT clusters.user_id) AS counter_value, "
        "TO_DATE(CURRENT_DATE) AS run_day   "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_applications_cert_managers_dedupe_source AS clusters_applications_cert_managers "
        "INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters "
        "ON clusters.id = clusters_applications_cert_managers.cluster_id "
        "WHERE clusters_applications_cert_managers.status IN (11, 3, 5)",
        "counts.clusters_platforms_eks": "SELECT 'counts.clusters_platforms_eks' AS counter_name,  "
        "COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters "
        "INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_cluster_providers_aws_dedupe_source AS cluster_providers_aws "
        "ON cluster_providers_aws.cluster_id = clusters.id "
        "WHERE clusters.provider_type = 2 "
        "AND (cluster_providers_aws.status IN (3)) "
        "AND clusters.enabled = TRUE",
        "counts.clusters_platforms_gke": "SELECT 'counts.clusters_platforms_gke' AS counter_name,  "
        "COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters "
        "INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_cluster_providers_gcp_dedupe_source AS cluster_providers_gcp "
        "ON cluster_providers_gcp.cluster_id = clusters.id "
        "WHERE clusters.provider_type = 1 "
        "AND (cluster_providers_gcp.status IN (3)) "
        "AND clusters.enabled = TRUE",
        "counts.clusters_platforms_user": "SELECT 'counts.clusters_platforms_user' AS counter_name,  "
        "COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters "
        "WHERE clusters.provider_type = 0 "
        "AND clusters.enabled = TRUE",
        "counts.incident_labeled_issues": "SELECT 'counts.incident_labeled_issues' AS counter_name,  "
        "COUNT(issues.id) AS counter_value, "
        "TO_DATE(CURRENT_DATE) AS run_day   "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_issues_dedupe_source AS issues "
        "INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_label_links_dedupe_source AS label_links "
        "ON label_links.target_type = 'Issue' "
        "AND label_links.target_id = issues.id "
        "INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_labels_dedupe_source AS labels "
        "ON labels.id = label_links.label_id "
        "WHERE labels.title = 'incident' "
        "AND labels.color = '#CC0033' "
        "AND labels.description = 'Denotes a disruption "
        "to IT services and the associated issues require immediate attention'",
    }

    final_sql_query_dict = main(test_cases_dict)

    for sql_metric, sql_query in final_sql_query_dict.items():
        #  check did we fix the bug with "JOINprep", should be fixed to "JOIN prep."
        final_sql = sql_query.upper()
        assert "JOINPREP" not in final_sql

        if "JOIN" in final_sql:
            assert "JOIN PREP" in final_sql

        # compare translated query with working SQL
        assert sql_query == results_dict[sql_metric]


def test_scalar_subquery():
    """
    Test case: Scalar subquery :: test SELECT (SELECT 1) -> SELECT (SELECT 1) as counter_value
    """
    test_cases_dict = {
        "counts": {
            "snippets": 'SELECT (SELECT COUNT("snippets"."id") FROM "snippets" WHERE "snippets"."type" = \'PersonalSnippet\') + (SELECT COUNT("snippets"."id") FROM "snippets" WHERE "snippets"."type" = \'ProjectSnippet\')'
        }
    }

    results_dict = {
        "counts.snippets": "SELECT 'counts.snippets' AS counter_name,  (SELECT COUNT(snippets.id) FROM prep.gitlab_dotcom.gitlab_dotcom_snippets_dedupe_source AS snippets WHERE snippets.type = 'PersonalSnippet') + (SELECT COUNT(snippets.id) FROM prep.gitlab_dotcom.gitlab_dotcom_snippets_dedupe_source AS snippets WHERE snippets.type = 'ProjectSnippet') AS counter_value, TO_DATE(CURRENT_DATE) AS run_day  "
    }

    final_sql_query_dict = main(test_cases_dict)

    for sql_metric, sql_query in final_sql_query_dict.items():
        # compare translated query with working SQL
        assert sql_query == results_dict[sql_metric]


def test_regular_subquery_transform():
    """
    Test case: regular subquery transform:
    SELECT a
      FROM (SELECT 1 as a
              FROM b) ->
    SELECT 'metric_name', a metric_value
      FROM (SELECT 1 AS a
              FROM b)
    """
    test_cases_dict_subquery: Dict[Any, Any] = {
        "usage_activity_by_stage_monthly": {
            "create": {
                "merge_requests_with_overridden_project_rules": 'SELECT COUNT(DISTINCT "approval_merge_request_rules"."merge_request_id") '
                'FROM "approval_merge_request_rules" '
                'WHERE "approval_merge_request_rules"."created_at" '
                "BETWEEN '2021-08-14 12:44:36.596707' AND '2021-09-11 12:44:36.596773' "
                "AND ((EXISTS (\n  SELECT\n    1\n  "
                "FROM\n    approval_merge_request_rule_sources\n  "
                "WHERE\n    approval_merge_request_rule_sources.approval_merge_request_rule_id = approval_merge_request_rules.id\n    "
                "AND NOT EXISTS (\n      "
                "SELECT\n        1\n      "
                "FROM\n        approval_project_rules\n      "
                "WHERE\n        approval_project_rules.id = approval_merge_request_rule_sources.approval_project_rule_id\n        "
                'AND EXISTS (\n          SELECT\n            1\n          FROM\n            projects\n          WHERE\n            projects.id = approval_project_rules.project_id\n            AND projects.disable_overriding_approvers_per_merge_request = FALSE))))\n    OR("approval_merge_request_rules"."modified_from_project_rule" = TRUE)\n)'
            }
        }
    }

    results_dict_subquery: Dict[Any, Any] = {
        "usage_activity_by_stage_monthly.create.merge_requests_with_overridden_project_rules": "SELECT 'usage_activity_by_stage_monthly.create.merge_requests_with_overridden_project_rules' "
        "AS counter_name,  "
        "COUNT(DISTINCT approval_merge_request_rules.merge_request_id) AS counter_value, "
        "TO_DATE(CURRENT_DATE) AS run_day   "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_approval_merge_request_rules_dedupe_source AS approval_merge_request_rules "
        "WHERE approval_merge_request_rules.created_at "
        "BETWEEN '2021-08-14 12:44:36.596707' AND '2021-09-11 12:44:36.596773' "
        "AND ((EXISTS ( SELECT 1 "
        "FROM prep.gitlab_dotcom.gitlab_dotcom_approval_merge_request_rule_sources_dedupe_source AS approval_merge_request_rule_sources WHERE approval_merge_request_rule_sources.approval_merge_request_rule_id = approval_merge_request_rules.id AND NOT EXISTS ( SELECT 1 FROM prep.gitlab_dotcom.gitlab_dotcom_approval_project_rules_dedupe_source AS approval_project_rules WHERE approval_project_rules.id = approval_merge_request_rule_sources.approval_project_rule_id AND EXISTS ( SELECT 1 FROM prep.gitlab_dotcom.gitlab_dotcom_projects_dedupe_source AS projects WHERE projects.id = approval_project_rules.project_id AND projects.disable_overriding_approvers_per_merge_request = FALSE)))) OR(approval_merge_request_rules.modified_from_project_rule = TRUE))"
    }

    final_sql_query_dict = main(test_cases_dict_subquery)

    for sql_metric, sql_query in final_sql_query_dict.items():
        # compare translated query with working SQL
        assert sql_query == results_dict_subquery[sql_metric]


def test_optimize_token_size():
    """
    Test case: optimize_token_size
    """
    test_cases_list: List[Any] = [
        "  SELECT  aa.bb  FROM   (SELECT   1 as aa) FROM BB ",
        "   SELECT 1",
        "   SELECT\n a from   bb   ",
        "",
        None,
    ]

    results_list: List[Any] = [
        "SELECT aa.bb FROM (SELECT 1 as aa) FROM BB",
        "SELECT 1",
        "SELECT a from bb",
        "",
        "",
    ]
    #

    for i, test_case_list in enumerate(test_cases_list):
        assert optimize_token_size(test_case_list) == results_list[i]


def test_count_pg_snowflake():
    """
    Test
    case: COUNT
    from Postgres to
    Snowflake: translate_postgres_snowflake_count
    """
    test_cases_list_count: List[Any] = [
        ["COUNT(DISTINCT aa.bb.cc)"],
        ["COUNT(xx.yy.zz)"],
        ["COUNT( DISTINCT oo.pp.rr)"],
        ["COUNT( xx.yy.zz)"],
        ["COUNT(users.users.id)"],
        None,
        [],
    ]

    results_list_count: List[Any] = [
        ["COUNT(DISTINCT bb.cc)"],
        ["COUNT(yy.zz)"],
        ["COUNT(DISTINCT pp.rr)"],
        ["COUNT(yy.zz)"],
        ["COUNT(users.id)"],
        [],
        [],
    ]

    for i, test_case_list_count in enumerate(test_cases_list_count):
        assert (
            translate_postgres_snowflake_count(test_case_list_count)
            == results_list_count[i]
        )


def test_find_keyword_index():
    """
    Test case: find_keyword_index
    """
    test_cases_parse: List[Any] = [
        sqlparse.parse("SELECT FROM")[0].tokens,
        sqlparse.parse("THERE IS NO MY WORD")[0].tokens,
        sqlparse.parse("THIS IS FROM SELECT")[0].tokens,
        sqlparse.parse("MORE SELECT AND ONE MORE FROM KEYWORD")[0].tokens,
    ]

    results_parse: List[int] = [0, 0, 6, 2]

    # 1. looking for SELECT keyword
    for i, test_case_parse in enumerate(test_cases_parse):
        assert find_keyword_index(test_case_parse, "SELECT") == results_parse[i]

    results_parse = [2, 0, 4, 10]
    # 2. looking for FROM keyword
    for i, test_case_parse in enumerate(test_cases_parse):
        assert find_keyword_index(test_case_parse, "FROM") == results_parse[i]


def test_prepare_sql_statement():
    """
    Test case: prepare_sql_statement
    """
    test_cases_prepare = [
        sqlparse.parse("SELECT 1")[0].tokens,
        sqlparse.parse("SELECT abc from def")[0].tokens,
        sqlparse.parse("SELECT (SELECT 1)")[0].tokens,
    ]

    results_list_prepare = ["SELECT 1", "SELECT abc from def", "SELECT (SELECT 1)"]

    for i, test_case_prepare in enumerate(test_cases_prepare):
        assert prepare_sql_statement(test_case_prepare) == results_list_prepare[i]


def test_static_variables():
    """
    Test case: check static variables
    """
    assert META_API_COLUMNS == [
        "recorded_at",
        "version",
        "edition",
        "recording_ce_finished_at",
        "recording_ee_finished_at",
        "uuid",
    ]
    assert TRANSFORMED_INSTANCE_QUERIES_FILE == "transformed_instance_queries.json"
    assert META_DATA_INSTANCE_QUERIES_FILE == "meta_data_instance_queries.json"


def test_keep_meta_data():
    """
    Test case: keep_meta_data
    """
    meta_json_result = {
        "recorded_at": "2022-01-21 09:07:46 UTC",
        "uuid": "1234",
        "hostname": "127.0.0.1",
        "version": "14.7.0-pre",
        "installation_type": "gitlab-development-kit",
        "active_user_count": "SELECT 1",
        "edition": "EE Free",
        "recording_ce_finished_at": "2022-01-21 09:07:53 UTC",
        "recording_ee_finished_at": "2022-01-21 09:07:53 UTC",
    }

    meta_json_raw = meta_json_result.copy()
    meta_json_raw["fake1"] = "12"
    meta_json_raw["fake2"] = "13"

    result_json = keep_meta_data(meta_json_raw)

    for result_key, result_value in result_json.items():
        assert result_value == meta_json_raw.get(result_key)

    assert isinstance(result_json, dict)
    assert len(result_json.keys()) == len(META_API_COLUMNS)
    assert result_json.get("fake1", None) is None
    assert result_json.get("fake2", None) is None


@pytest.fixture
def test_cases_dict_transformed() -> Dict[Any, Any]:
    """
    Prepare fixture data for testing
    """
    test_cases_dict_subquery: Dict[Any, Any] = {
        "usage_activity_by_stage_monthly": {
            "create": {
                "approval_project_rules_with_more_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) > approvals_required)) subquery',
                "approval_project_rules_with_less_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) < approvals_required)) subquery',
                "approval_project_rules_with_exact_required_approvers": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) = approvals_required)) subquery',
            }
        },
        "usage_activity_by_stage": {
            "create": {
                "approval_project_rules_with_more_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) > approvals_required)) subquery',
                "approval_project_rules_with_less_approvers_than_required": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) < approvals_required)) subquery',
                "approval_project_rules_with_exact_required_approvers": 'SELECT COUNT(*) FROM (SELECT COUNT("approval_project_rules"."id") FROM "approval_project_rules" INNER JOIN approval_project_rules_users ON approval_project_rules_users.approval_project_rule_id = approval_project_rules.id WHERE "approval_project_rules"."rule_type" = 0 GROUP BY "approval_project_rules"."id" HAVING (COUNT(approval_project_rules_users) = approvals_required)) subquery',
            }
        },
    }

    return main(test_cases_dict_subquery)


@pytest.fixture
def metric_list() -> set:
    """
    static list of metrics for testing complex subqueries
    """
    return {
        "usage_activity_by_stage_monthly.create.approval_project_rules_with_more_approvers_than_required",
        "usage_activity_by_stage_monthly.create.approval_project_rules_with_less_approvers_than_required",
        "usage_activity_by_stage_monthly.create.approval_project_rules_with_exact_required_approvers",
        "usage_activity_by_stage.create.approval_project_rules_with_more_approvers_than_required",
        "usage_activity_by_stage.create.approval_project_rules_with_less_approvers_than_required",
        "usage_activity_by_stage.create.approval_project_rules_with_exact_required_approvers",
    }


def test_subquery_complex(test_cases_dict_transformed, metric_list):
    """
    Test bugs we found for complex subquery with FROM () clause
    """
    expect_value_select, expect_value_from = 2, 2

    final_sql_query_dict = test_cases_dict_transformed

    for metric_name, metric_sql in final_sql_query_dict.items():
        assert "approval" in metric_name
        assert metric_name in metric_list
        assert metric_name in metric_sql

        assert (
            metric_sql.upper().count("SELECT") == expect_value_select
        )  # expect 2 SELECT statement
        assert (
            metric_sql.upper().count("FROM") == expect_value_from
        )  # expect 2 FROM statements
        assert metric_sql.count("(") == metric_sql.count(")")  # query parsed properly


def test_transform_having_clause(test_cases_dict_transformed, metric_list):
    """
    Test bugs we found for complex subquery - having clause

    bug fixing, need to move from:
    (COUNT(approval_project_rules_users) < approvals_required)
    to
    (COUNT(approval_project_rules_users.id) < MAX(approvals_required))
    """

    final_sql_query_dict = test_cases_dict_transformed

    for metric_name, metric_sql in final_sql_query_dict.items():
        assert ".id" in metric_sql
        assert "MAX(" in metric_sql
        assert "COUNT(approval_project_rules_users.id)" in metric_sql
        assert "MAX(approvals_required)" in metric_sql
        assert "subquery" in metric_sql
        assert metric_sql.count("(") == metric_sql.count(")")
        assert metric_name in metric_list
        assert metric_name in metric_sql


def test_constants():
    assert TRANSFORMED_INSTANCE_QUERIES_FILE is not None
    assert META_DATA_INSTANCE_QUERIES_FILE is not None
    assert HAVING_CLAUSE_PATTERN is not None
    assert METRICS_EXCEPTION is not None


if __name__ == "__main__":
    test_transforming_queries()
    test_scalar_subquery()
    test_regular_subquery_transform()
    test_optimize_token_size()
    test_count_pg_snowflake()
    test_find_keyword_index()
    test_prepare_sql_statement()
    test_static_variables()
    test_keep_meta_data()
    test_subquery_complex(test_cases_dict_transformed, metric_list)
    test_transform_having_clause(test_cases_dict_transformed, metric_list)
    test_constants()
