{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref ('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=['']) %}

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

    SELECT *
    FROM {{ ref('version_usage_data_unpacked_stats_used') }}
    {% if is_incremental() %}

      WHERE created_at >= (SELECT MAX(created_at) FROM {{this}})

    {% endif %}
    
), pivoted AS (

    SELECT 
        id,
        {{-
            dbt_utils.pivot(
                column='full_ping_name',
                values=dbt_utils.get_column_values(table=ref ('version_usage_stats_list'), column='full_ping_name', default=['']),
                agg='MAX',
                then_value='IFF(ping_value = -1 ,NULL, ping_value)',
                else_value='NULL',
                quote_identifiers=false
            )
        -}}
    FROM stats_used_unpacked
    GROUP BY id

), pivoted_v2 AS (

    SELECT 
        *        
    FROM stats_used_unpacked
        PIVOT (MAX(IFF(ping_value = -1 ,NULL, ping_value))
            FOR full_ping_name IN ({{ '\'' + version_usage_stats_list|join('\',\n \'') + '\'' }}))
            AS pioved_table (id, {{ '\n' + version_usage_stats_list|join(',\n') }})

), unpacked AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data_with_metadata'), except=["STATS_USED", "COUNTS", "USAGE_ACTIVITY_BY_STAGE", "ANALYTICS_UNIQUE_VISIT"], relation_alias='usage_data') }},
      ping_name,
      full_ping_name,
      ping_value

    FROM usage_data
    LEFT JOIN stats_used_unpacked
      ON usage_data.id = stats_used_unpacked.id

), final AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data_with_metadata'), except=["STATS_USED", "COUNTS", "USAGE_ACTIVITY_BY_STAGE", "RAW_USAGE_DATA_PAYLOAD", "ANALYTICS_UNIQUE_VISITS"]) }},
      {% for stat_name in version_usage_stats_list %}
        MAX(IFF(full_ping_name = '{{stat_name}}', ping_value::NUMBER, NULL)) AS {{stat_name}}
        {{ "," if not loop.last }}
      {% endfor %}
    FROM unpacked
    {{ dbt_utils.group_by(n=72) }}


)

SELECT *
FROM final
