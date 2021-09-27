WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit_type') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      credit_type_id,
      descr,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      name

    FROM source
    
)

SELECT *
FROM renamed