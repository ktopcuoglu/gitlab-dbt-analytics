WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_send_alert') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      send_to_owner                     AS send_to_owner,
      send_to_list                      AS send_to_list

    FROM source

)

SELECT *
FROM renamed
