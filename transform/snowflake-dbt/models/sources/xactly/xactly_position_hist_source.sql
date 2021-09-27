WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_position_hist') }}

), renamed AS (

    SELECT

      business_group_id,
      created_by_id,
      created_by_name,
      created_date,
      credit_end_date,
      credit_start_date,
      descr,
      effective_end_date,
      effective_start_date,
      incent_end_date,
      incent_st_date,
      is_active,
      is_master,
      master_position_id,
      modified_by_id,
      modified_by_name,
      modified_date,
      name,
      object_id,
      parent_position_id,
      parent_record_id,
      participant_id,
      pos_group_id,
      position_id,
      title_id

    FROM source

)

SELECT *
FROM renamed