WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_hierarchy_hist') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      from_pos_id,
      md5(from_pos_name) AS from_pos_uuid,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      object_id,
      pos_hierarchy_id,
      pos_hierarchy_type_id,
      to_pos_id,
      md5(to_pos_name) AS to_pos_uuid,
      version,
      _loaded_at

    FROM source
)

SELECT *
FROM renamed