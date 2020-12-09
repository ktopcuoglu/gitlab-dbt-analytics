{%- macro create_pi_source_table(source_performance_indicator) -%}

WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source_performance_indicator }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      date_trunc('day', uploaded_at)::date    AS snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), intermediate_stage AS (

     SELECT 
      data_by_row['name']::VARCHAR                         AS pi_name,
      data_by_row['org']::VARCHAR                          AS org_name,
      data_by_row['definition']::VARCHAR                   AS pi_definition,
      data_by_row['is_key']::BOOLEAN                       AS is_key,
      data_by_row['is_primary']::BOOLEAN                   AS is_primary,
      data_by_row['public']::BOOLEAN                       AS is_public,
      data_by_row['sisense_data'] IS NOT NULL              AS is_embedded,
      data_by_row['target']::VARCHAR                       AS pi_target,
      data_by_row['telemetry_type']::VARCHAR               AS telemetry_type,
      data_by_row['urls']::VARCHAR                         AS pi_url,
      data_by_row['sisense_data'].chart::VARCHAR           AS sisense_chart_id,
      data_by_row['sisense_data'].dashboard::VARCHAR       AS sisense_dashboard_id,
      snapshot_date,
      rank
    FROM intermediate

)

SELECT *
FROM intermediate_stage


 {% endmacro %}
