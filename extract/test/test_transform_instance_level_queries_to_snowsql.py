import pytest

from extract.saas_usage_ping.transform_instance_level_queries_to_snowsql import (
    sql_queries_dict,
    add_counter_name_as_column,
    rename_query_tables,
)

input_raw_sql = {
    "counts": {
        "boards": 'SELECT COUNT("boards"."id") FROM "boards"',
        "clusters_applications_cert_managers": 'SELECT COUNT(DISTINCT "clusters_applications_cert_managers"."clusters.user_id") FROM "clusters_applications_cert_managers" INNER JOIN "clusters" ON "clusters"."id" = "clusters_applications_cert_managers"."cluster_id" WHERE "clusters_applications_cert_managers"."status" IN (11, 3, 5)',
        "clusters_platforms_eks": 'SELECT COUNT("clusters"."id") FROM "clusters" INNER JOIN "cluster_providers_aws" ON "cluster_providers_aws"."cluster_id" = "clusters"."id" WHERE "clusters"."provider_type" = 2 AND ("cluster_providers_aws"."status" IN (3)) AND "clusters"."enabled" = TRUE',
        "clusters_platforms_gke": 'SELECT COUNT("clusters"."id") FROM "clusters" INNER JOIN "cluster_providers_gcp" ON "cluster_providers_gcp"."cluster_id" = "clusters"."id" WHERE "clusters"."provider_type" = 1 AND ("cluster_providers_gcp"."status" IN (3)) AND "clusters"."enabled" = TRUE',
        "clusters_platforms_user": 'SELECT COUNT("clusters"."id") FROM "clusters" WHERE "clusters"."provider_type" = 0 AND "clusters"."enabled" = TRUE',
        "incident_labeled_issues": 'SELECT COUNT("issues"."id") FROM "issues" INNER JOIN "label_links" ON "label_links"."target_type" = \'Issue\' AND "label_links"."target_id" = "issues"."id" INNER JOIN "labels" ON "labels"."id" = "label_links"."label_id" WHERE "labels"."title" = \'incident\' AND "labels"."color" = \'#CC0033\' AND "labels"."description" = \'Denotes a disruption to IT services and the associated issues require immediate attention\'',
    }
}

output_translated_sql = {
    "counts.boards": "SELECT 'counts.boards' AS counter_name,  COUNT(boards.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_boards_dedupe_source AS boards",
    "counts.clusters_applications_cert_managers": "SELECT 'counts.clusters_applications_cert_managers' AS counter_name,  COUNT(DISTINCT clusters_applications_cert_managers.clusters.user_id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_applications_cert_managers_dedupe_source AS clusters_applications_cert_managers INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters ON clusters.id = clusters_applications_cert_managers.cluster_id WHERE clusters_applications_cert_managers.status IN (11, 3, 5)",
    "counts.clusters_platforms_eks": "SELECT 'counts.clusters_platforms_eks' AS counter_name,  COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_cluster_providers_aws_dedupe_source AS cluster_providers_aws ON cluster_providers_aws.cluster_id = clusters.id WHERE clusters.provider_type = 2 AND (cluster_providers_aws.status IN (3)) AND clusters.enabled = TRUE",
    "counts.clusters_platforms_gke": "SELECT 'counts.clusters_platforms_gke' AS counter_name,  COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_cluster_providers_gcp_dedupe_source AS cluster_providers_gcp ON cluster_providers_gcp.cluster_id = clusters.id WHERE clusters.provider_type = 1 AND (cluster_providers_gcp.status IN (3)) AND clusters.enabled = TRUE",
    "counts.clusters_platforms_user": "SELECT 'counts.clusters_platforms_user' AS counter_name,  COUNT(clusters.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_clusters_dedupe_source AS clusters WHERE clusters.provider_type = 0 AND clusters.enabled = TRUE",
    "counts.incident_labeled_issues": "SELECT 'counts.incident_labeled_issues' AS counter_name,  COUNT(issues.id) AS counter_value, TO_DATE(CURRENT_DATE) AS run_day   FROM prep.gitlab_dotcom.gitlab_dotcom_issues_dedupe_source AS issues INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_label_links_dedupe_source AS label_links ON label_links.target_type = 'Issue' AND label_links.target_id = issues.id INNER JOIN prep.gitlab_dotcom.gitlab_dotcom_labels_dedupe_source AS labels ON labels.id = label_links.label_id WHERE labels.title = 'incident' AND labels.color = '#CC0033' AND labels.description = 'Denotes a disruption to IT services and the associated issues require immediate attention'",
}

# Start: piece of the code from the original file
sql_queries_dictionary = sql_queries_dict(
    input_raw_sql
)  # read from test case instead of API

sql_queries_dict_with_new_column = {
    metric_name: add_counter_name_as_column(
        metric_name, sql_queries_dictionary[metric_name]
    )
    for metric_name in sql_queries_dictionary
}

final_sql_query_dict = {
    metric_name: rename_query_tables(sql_queries_dict_with_new_column[metric_name])
    for metric_name in sql_queries_dict_with_new_column
}
# End: piece of the code from the original file

# Test cases
for sql_metric, sql_query in final_sql_query_dict.items():
    #  check did we fix the bug with "JOINprep", should be fixed to "JOIN prep."
    final_sql = sql_query.upper()
    assert not "JOINPREP" in final_sql

    if "JOIN" in final_sql:
        assert "JOIN PREP"

    # compare translated query with working SQL
    assert sql_query == output_translated_sql[sql_metric]
