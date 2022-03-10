WITH
{{ distinct_source(source=source('gitlab_dotcom', 'group_import_states')) }}

, base AS (

    SELECT *
    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='group_id',
    primary_key_raw='group_id',
    source_cte='distinct_source',
    casted_cte='base'
) }}
