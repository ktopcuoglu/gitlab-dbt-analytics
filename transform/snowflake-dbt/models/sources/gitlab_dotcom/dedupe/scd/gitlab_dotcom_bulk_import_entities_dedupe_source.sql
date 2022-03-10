WITH
{{ distinct_source(source=source('gitlab_dotcom', 'bulk_import_entities')) }}

, base AS (

    SELECT *
    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='id',
    primary_key_raw='id',
    source_cte='distinct_source',
    casted_cte='base'
) }}
