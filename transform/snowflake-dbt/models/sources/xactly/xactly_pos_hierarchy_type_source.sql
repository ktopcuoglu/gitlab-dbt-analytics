WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_hierarchy_type') }}

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
      pos_hierarchy_type_disp_name,
      pos_hierarchy_type_id,
      pos_hierarchy_type_name

    FROM source
    
)

SELECT *
FROM renamed