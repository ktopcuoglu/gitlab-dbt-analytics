WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_part_assignment') }}

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
      md5(participant_name) AS participant_uuid,
      pos_part_assignment_id,
      position_id,
      md5(position_name) AS position_uuid

    FROM source
    
)

SELECT *
FROM renamed