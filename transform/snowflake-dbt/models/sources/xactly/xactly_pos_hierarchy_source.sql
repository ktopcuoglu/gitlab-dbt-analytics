WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_hierarchy') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      from_pos_id,
      from_pos_name,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      pos_hierarchy_id,
      pos_hierarchy_type_id,
      to_pos_id,
      to_pos_name

    FROM source
    
)

SELECT *
FROM renamed