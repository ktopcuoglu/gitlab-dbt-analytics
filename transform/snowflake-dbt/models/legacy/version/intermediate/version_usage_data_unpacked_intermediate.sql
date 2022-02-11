{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref ('version_usage_stats_list'), column='full_ping_name', default=['']) %}

{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

WITH usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data_with_metadata') }}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}

), stats_used_unpacked AS (

    SELECT 
        id,
        full_ping_name,
        ping_value
    FROM {{ ref('version_usage_data_unpacked_stats_used') }}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}
    
), pivoted AS (

    SELECT 
        *        
    FROM stats_used_unpacked
        PIVOT (MAX(ping_value)
            FOR full_ping_name IN ({{ '\'' + version_usage_stats_list|join('\',\n \'') + '\'' }}))
            AS pivoted_table (id, {{ '\n' + version_usage_stats_list|join(',\n') }})

), final AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data_with_metadata'), except=["ID", "STATS_USED", "COUNTS", "USAGE_ACTIVITY_BY_STAGE", "ANALYTICS_UNIQUE_VISIT"], relation_alias='usage_data') }},
      pivoted.*

    FROM usage_data
    LEFT JOIN pivoted
      ON usage_data.id = pivoted.id

)

SELECT *
FROM final
