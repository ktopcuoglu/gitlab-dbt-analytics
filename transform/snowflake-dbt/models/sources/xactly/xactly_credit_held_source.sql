WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit_held') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      credit_held_id,
      credit_id,
      held_date,
      is_active,
      is_held,
      modified_by_id,
      modified_by_name,
      modified_date,
      release_group_id,
      run_id,
      trans_id

    FROM source
    
)

SELECT *
FROM renamed