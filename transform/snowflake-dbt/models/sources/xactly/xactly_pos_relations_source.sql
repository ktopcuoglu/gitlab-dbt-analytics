WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_relations') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      from_pos_id,
      from_pos_name,
      id,
      modified_by_id,
      modified_by_name,
      modified_date,
      pos_rel_type_id,
      to_pos_id,
      to_pos_name

    FROM source
    
)

SELECT *
FROM renamed