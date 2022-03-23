WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_email_bounced_soft') }}

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
      category                          AS category,
      email                             AS email,
      details                           AS details,
      subcategory                       AS subcategory,
      step_id                           AS step_id,
      choice_number                     AS choice_number,
      _fivetran_synced                  AS _fivetran_synced,
      test_variant                      AS test_variant

    FROM source

)

SELECT *
FROM renamed
