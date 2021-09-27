WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_role') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      descr,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      name,
      role_id,
      role_type

    FROM source

)

SELECT *
FROM renamed