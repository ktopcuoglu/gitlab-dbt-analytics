WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_attainment_measure_criteria') }}

), renamed AS (

    SELECT

      attainment_measure_criteria_id, 
      attainment_measure_id,
      created_by_id,
      created_date,
      criteria_id,
      history_uuid,
      is_active,
      modified_by_id,
      modified_date,
      name,
      type

    FROM source
    
)

SELECT *
FROM renamed