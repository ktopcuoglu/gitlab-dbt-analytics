WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_quota_relationship') }}

), renamed AS (

    SELECT

      created_by_id,
      created_date,
      description,
      is_active,
      label,
      modified_by_id,
      modified_date,
      name,
      quota_relationship_id

    FROM source
    
)

SELECT *
FROM renamed