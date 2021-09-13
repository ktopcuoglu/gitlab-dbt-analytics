import pytest

from extract.saas_usage_ping.transform_instance_level_queries_to_snowsql import (
    sql_queries_dict,
    add_counter_name_as_column,
    rename_query_tables,
)

test_input_raw = {
    "counts": {
        "clusters_platforms_eks": 'SELECT COUNT("clusters"."id") FROM "clusters" INNER JOIN "cluster_providers_aws" ON "cluster_providers_aws"."cluster_id" = "clusters"."id" WHERE "clusters"."provider_type" = 2 AND ("cluster_providers_aws"."status" IN (3)) AND "clusters"."enabled" = TRUE',
        "clusters_platforms_gke": 'SELECT COUNT("clusters"."id") FROM "clusters" INNER JOIN "cluster_providers_gcp" ON "cluster_providers_gcp"."cluster_id" = "clusters"."id" WHERE "clusters"."provider_type" = 1 AND ("cluster_providers_gcp"."status" IN (3)) AND "clusters"."enabled" = TRUE',
        "clusters_platforms_user": 'SELECT COUNT("clusters"."id") FROM "clusters" WHERE "clusters"."provider_type" = 0 AND "clusters"."enabled" = TRUE',
    }
}

#  TODO: rbacovic - test case for missing whitespace " "

#  TODO: rbacovic - test case for double alias

#  TODO: rbacovic - test case for missing table
