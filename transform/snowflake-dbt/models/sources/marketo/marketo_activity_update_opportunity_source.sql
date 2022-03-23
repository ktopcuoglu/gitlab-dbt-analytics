WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_update_opportunity') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      data_value_changes                AS data_value_changes

    FROM source

)

SELECT *
FROM renamed
