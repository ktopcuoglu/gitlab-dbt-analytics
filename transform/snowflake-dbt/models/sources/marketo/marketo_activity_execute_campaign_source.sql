WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_execute_campaign') }}

), renamed AS (

    SELECT

      id                                        AS id,
      lead_id                                   AS lead_id,
      activity_date                             AS activity_date,
      activity_type_id                          AS activity_type_id,
      campaign_id                               AS campaign_id,
      primary_attribute_value_id                AS primary_attribute_value_id,
      primary_attribute_value                   AS primary_attribute_value,
      used_parent_campaign_token_context        AS used_parent_campaign_token_context,
      qualified                                 AS qualified,
      _fivetran_synced                          AS _fivetran_synced

    FROM source

)

SELECT *
FROM renamed
