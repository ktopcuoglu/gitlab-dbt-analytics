WITH source AS (
  SELECT *
  FROM {{ source('workday','employment_status') }}
),

renamed AS (

  SELECT
    source.employee_id::NUMBER AS employee_id,
    source._fivetran_synced::TIMESTAMP AS uploaded_at,
    events.value['EFFECTIVE_DATE']::DATE AS effective_date,
    events.value['EMPLOYMENT_STATUS']::VARCHAR AS employment_status,
    events.value['TERMINATION_TYPE']::VARCHAR AS termination_type
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => source.employment_status) AS events

)

SELECT *
FROM renamed
