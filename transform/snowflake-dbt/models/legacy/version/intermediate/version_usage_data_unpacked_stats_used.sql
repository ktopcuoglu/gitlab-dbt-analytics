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
    REPLACE(f.path, '.','_')                                                        AS full_ping_name,
    f.value                                                                         AS ping_value

FROM joined,
    lateral flatten(input => joined.stats_used, recursive => True) f
WHERE IS_OBJECT(f.value) = FALSE
    AND stats_used IS NOT NULL

{% if is_incremental() %}
    AND created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}
