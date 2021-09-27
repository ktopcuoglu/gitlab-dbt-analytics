WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_attainment_measure') }}

), renamed AS (

    SELECT

      attainment_measure_id,
      created_by_id,
      created_date,
      description,
      effective_end_period_id,
      effective_start_period_id,
      history_uuid,
      is_active,
      master_attainment_measure_id,
      modified_by_id,
      modified_date,
      name,
      period_type

    FROM source
    
)

SELECT *
FROM renamed