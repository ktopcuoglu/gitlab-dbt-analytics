{% set version_usage_stats_list = dbt_utils.get_column_values(table=ref ('version_usage_stats_list'), column='full_ping_name', max_records=1000, default=['']) %}

{{ config({
    "materialized": "incremental",
    "unique_key": "id"
    })
}}

WITH usage_data AS (

    SELECT *
    FROM {{ ref('version_usage_data_with_metadata') }}

), unpacked AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data_with_metadata'), except=["STATS_USED", "COUNTS", "USAGE_ACTIVITY_BY_STAGE", "ANALYTICS_UNIQUE_VISIT"], relation_alias='version_usage_data_with_metadata') }},
      ping_name,
      full_ping_name,
      ping_value

    FROM usage_data
    LEFT JOIN stats_used_unpacked
      ON joined.id = stats_used_unpacked.id

), final AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data_with_metadata'), except=["STATS_USED", "COUNTS", "USAGE_ACTIVITY_BY_STAGE", "RAW_USAGE_DATA_PAYLOAD", "ANALYTICS_UNIQUE_VISITS"]) }},
      {% for stat_name in version_usage_stats_list %}
        MAX(IFF(full_ping_name = '{{stat_name}}', ping_value::NUMBER, NULL)) AS {{stat_name}}
        {{ "," if not loop.last }}
      {% endfor %}
    FROM unpacked
    {{ dbt_utils.group_by(n=70) }}


)

SELECT *
FROM final
