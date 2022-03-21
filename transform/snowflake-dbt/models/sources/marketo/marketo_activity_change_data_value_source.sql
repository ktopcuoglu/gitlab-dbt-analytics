WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_change_data_value') }}

), renamed AS (

    SELECT
      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      old_value                         AS old_value,
      new_value                         AS new_value,
      reason                            AS reason,
      source                            AS source,
      _fivetran_synced                  AS _fivetran_synced,
      api_method_name                   AS api_method_name,
      modifying_user                    AS modifying_user,
      request_id                        AS request_id
    FROM source

)

SELECT *
FROM renamed
