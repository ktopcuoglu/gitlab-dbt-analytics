WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_delete_lead') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      campaign                          AS campaign,
      _fivetran_synced                  AS _fivetran_synced

    FROM source

)

SELECT *
FROM renamed
