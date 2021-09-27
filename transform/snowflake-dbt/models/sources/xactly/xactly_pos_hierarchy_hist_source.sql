WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_hierarchy_hist') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      from_pos_id,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      object_id,
      pos_hierarchy_id,
      pos_hierarchy_type_id,
      to_pos_id,
      {{ nohash_sensitive_columns('from_pos_name', 'to_pos_name') }}

    FROM source
    
)

SELECT *
FROM renamed