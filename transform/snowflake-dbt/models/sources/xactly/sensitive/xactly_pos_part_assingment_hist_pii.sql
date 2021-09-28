WITH source AS (

    SELECT *
    FROM {{ red('xactly_pos_part_assignment_hist_source') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      object_id,
      participant_id,
      pos_part_assignment_id,
      position_id,
      {{ nohash_sensitive_columns('participant_name', 'position_name') }}

    FROM source
    
)

SELECT *
FROM renamed