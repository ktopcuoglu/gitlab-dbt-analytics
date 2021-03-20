WITH source AS (

    SELECT *
    FROM {{ ref('airflow_dag') }}

), renamed AS (

    SELECT
      dag_id,
      is_active,
      is_paused,
      schedule_interval
    FROM source

)

SELECT *
FROM renamed