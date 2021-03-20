WITH source AS (

    SELECT *
    FROM {{ source('airflow_dag_run') }}

), renamed AS (

    SELECT
      dag_id,
      execution_date,
      run_state
    FROM source

)

SELECT *
FROM renamed