WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_quota') }}

), renamed AS (

    SELECT

      classification_name,
      classification_type,
      created_by_id,
      created_by_name,
      created_date,
      description,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      name,
      quota_id,
      quota_interval_id,
      quota_interval_name,
      quota_period_id,
      quota_period_name,
      quota_value,
      quota_value_unit_type_id,
      quota_value_unit_type_name,
      source_id

    FROM source
    
)

SELECT *
FROM renamed