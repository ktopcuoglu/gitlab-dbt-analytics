WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_change_score') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      _fivetran_synced                  AS _fivetran_synced,
      change_value                      AS change_value,
      old_value                         AS old_value,
      new_value                         AS new_value,
      reason                            AS reason,
      relative_urgency                  AS relative_urgency,
      priority                          AS priority,
      relative_score                    AS relative_score,
      urgency                           AS urgency

    FROM source

)

SELECT *
FROM renamed
