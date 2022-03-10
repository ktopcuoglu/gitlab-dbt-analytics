WITH
{{ distinct_source(source=source('gitlab_dotcom', 'clusters_integration_prometheus')) }}

, base AS (

    SELECT *
    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='cluster_id',
    primary_key_raw='cluster_id',
    source_cte='distinct_source',
    casted_cte='base'
) }}