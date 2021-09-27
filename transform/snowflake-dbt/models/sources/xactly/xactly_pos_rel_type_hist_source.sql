WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_rel_type_hist') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      descr,
      effective_end_date,
      effective_start_date,
      is_active,
      is_master,
      modified_by_id,
      modified_by_name,
      modified_date,
      name,
      object_id,
      pos_rel_type_id

    FROM source
    
)

SELECT *
FROM renamed