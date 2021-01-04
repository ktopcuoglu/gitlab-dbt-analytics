WITH audit_results AS (

    SELECT
      sensor_name,
      time_detected::DATE AS time_detected_date,
      database_name
    FROM {{ ref('datasiren_audit_results') }}

)

SELECT *
FROM audit_results
