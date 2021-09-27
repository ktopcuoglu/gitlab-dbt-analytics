WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit_totals') }}

), renamed AS (

    SELECT

      amount,
      created_by_id,
      created_by_name,
      credit_totals_id,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      participant_id,
      participant_name,
      period_id,
      position_id,
      unittype_id

    FROM source
    
)

SELECT *
FROM renamed