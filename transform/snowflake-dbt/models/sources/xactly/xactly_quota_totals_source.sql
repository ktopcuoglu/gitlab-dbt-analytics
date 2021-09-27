WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_quota_totals') }}

), renamed AS (

    SELECT

      amount,
      created_by_id,
      created_by_name,
      created_date,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      name,
      participant_id,
      participant_name,
      period_id,
      position_id,
      position_name,
      quota_totals_id,
      unittype_id

    FROM source

)

SELECT *
FROM renamed