from typing import Dict, List, Any
import pytest
import sqlparse.sql

from extract.saas_usage_ping.transform_instance_level_queries_to_snowsql import (
    main,
    optimize_token_size,
    find_keyword_index,
    prepare_sql_statement,
    translate_postgres_snowflake_count,
)

##################################################################
# Test case: for transforming queries from Postgres to Snowflake
##################################################################
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
    assert not "JOINPREP" in final_sql

    if "JOIN" in final_sql:
        assert "JOIN PREP" in final_sql

    # compare translated query with working SQL
    assert sql_query == results_dict[sql_metric]

##################################################################
# Test case: Scalar subquery :: test SELECT (SELECT 1) -> SELECT (SELECT 1) as counter_value
##################################################################
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

##################################################################
# Test case: regular subquery transform:
# SELECT a
#   FROM (SELECT 1 as a
#           FROM b) ->
# SELECT 'metric_name', a metric_value
#   FROM (SELECT 1 AS a
#           FROM b)
##################################################################
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

##################################################################
# Test case: optimize_token_size
##################################################################
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

##################################################################
# Test case: COUNT from PG to Snowflake: translate_postgres_snowflake_count
##################################################################
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

##################################################################
# Test case: find_keyword_index
##################################################################
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

##################################################################
# Test case: prepare_sql_statement
##################################################################
test_cases_prepare = [
    sqlparse.parse("SELECT 1")[0].tokens,
    sqlparse.parse("SELECT abc from def")[0].tokens,
    sqlparse.parse("SELECT (SELECT 1)")[0].tokens,
]

results_list_prepare = ["SELECT 1", "SELECT abc from def", "SELECT (SELECT 1)"]

for i, test_case_prepare in enumerate(test_cases_prepare):
    assert prepare_sql_statement(test_case_prepare) == results_list_prepare[i]
