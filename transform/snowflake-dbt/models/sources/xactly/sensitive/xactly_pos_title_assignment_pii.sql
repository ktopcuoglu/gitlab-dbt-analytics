WITH source AS (

    SELECT *
    FROM {{ ref('xactly_pos_title_assignment_source') }}

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
      title_id,
      title_name,
      {{ nohash_sensitive_columns('position_name') }}

    FROM source
    
)

SELECT *
FROM renamed