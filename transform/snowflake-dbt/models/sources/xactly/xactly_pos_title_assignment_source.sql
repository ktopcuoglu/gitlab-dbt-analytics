WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_title_assignment') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      pos_title_assignment_id,
      position_id,
      position_name,
      title_id,
      title_name

    FROM source
    
)

SELECT *
FROM renamed