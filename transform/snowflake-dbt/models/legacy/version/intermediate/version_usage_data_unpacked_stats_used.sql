{{ config({
    "materialized": "incremental",
    "unique_key": "id_full_ping_name"
    })
}}

WITH usage_data AS (

    SELECT {{ dbt_utils.star(from=ref('version_usage_data'), except=["LICENSE_STARTS_AT", "LICENSE_EXPIRES_AT"]) }}
    FROM {{ ref('version_usage_data') }}

)

SELECT
  {{ dbt_utils.surrogate_key(['id', 'path']) }}                                    AS id_full_ping_name,
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
