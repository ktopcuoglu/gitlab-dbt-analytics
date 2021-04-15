{{ config({
        "materialized": "table"
    })
}}

WITH datasiren_summary AS (

    SELECT *
    FROM {{ ref('datasiren_audit_results') }}

), grouped AS (

    SELECT

      sensor_name,
      database_name,
      table_schema,
      table_name,
      column_name,
      COUNT(DISTINCT other_identifier) AS rows_detected,
      MAX(time_detected)               AS last_detected,
      MIN(time_detected)               AS first_detected
    
		FROM datasiren_summary
    {{ dbt_utils.group_by(n=5) }}

)

SELECT * 
FROM grouped
