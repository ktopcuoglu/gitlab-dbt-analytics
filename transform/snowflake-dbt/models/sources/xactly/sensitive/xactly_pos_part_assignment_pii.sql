WITH source AS (

    SELECT *
    FROM {{ ref('xactly_pos_part_assignment_source') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      participant_id,
      {{ nohash_sensitive_columns('xactly_pos_part_assignment_source', 'participant_name') }},
      pos_part_assignment_id,
      position_id,
      {{ nohash_sensitive_columns('xactly_pos_part_assignment_source', 'position_name') }}

    FROM source
    
)

SELECT *
FROM renamed