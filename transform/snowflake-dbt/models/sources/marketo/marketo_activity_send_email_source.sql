WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_send_email') }}

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
      step_id                           AS step_id,
      choice_number                     AS choice_number,
      test_variant                      AS test_variant

    FROM source

)

SELECT *
FROM renamed
