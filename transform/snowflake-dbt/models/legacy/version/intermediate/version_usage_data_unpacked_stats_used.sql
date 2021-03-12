{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

WITH usage_data AS (

    SELECT {{ dbt_utils.star(from=ref('version_usage_data'), except=["LICENSE_STARTS_AT", "LICENSE_EXPIRES_AT"]) }}
    FROM {{ ref('version_usage_data') }}

)

SELECT
    id,
    f.path                                                                          AS ping_name,
    created_at,
    REPLACE(f.path, '.','_')                                                        AS full_ping_name,
    f.value                                                                         AS ping_value

FROM usage_data,
    lateral flatten(input => usage_data.stats_used, recursive => True) f
WHERE IS_OBJECT(f.value) = FALSE
    AND stats_used IS NOT NULL
    AND full_ping_name IN (SELECT full_ping_name FROM {{ ref('version_usage_stats_list') }})
{% if is_incremental() %}
    AND created_at >= (SELECT MAX(created_at) FROM {{ this }})
{% endif %}
