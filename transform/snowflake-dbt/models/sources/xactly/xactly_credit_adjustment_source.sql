WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit_adjustment') }}

), renamed AS (

    SELECT

      amount,
      amount_unit_type_id,
      created_by_id,
      created_by_name,
      created_date,
      credit_adjustment_id,
      credit_id,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      period_id,
      reason_id

    FROM source
    
)

SELECT *
FROM renamed