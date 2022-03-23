WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_open_email') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      campaign_run_id                   AS campaign_run_id,
      platform                          AS platform,
      is_mobile_device                  AS is_mobile_device,
      step_id                           AS step_id,
      device                            AS device,
      test_variant                      AS test_variant,
      choice_number                     AS choice_number,
      is_bot_activity                   AS is_bot_activity,
      user_agent                        AS user_agent,
      bot_activity_pattern              AS bot_activity_pattern
    FROM source

)

SELECT *
FROM renamed
