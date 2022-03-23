WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_fill_out_linkedin_lead_gen_form') }}

), renamed AS (

    SELECT

      id                            AS id,
      lead_id                       AS lead_id,
      activity_date                 AS activity_date,
      activity_type_id              AS activity_type_id,
      campaign_id                   AS campaign_id,
      primary_attribute_value_id    AS primary_attribute_value_id,
      primary_attribute_value       AS primary_attribute_value,
      lead_gen_campaign_name        AS lead_gen_campaign_name,
      lead_gen_creative_id          AS lead_gen_creative_id,
      lead_gen_account_name         AS lead_gen_account_name,
      _fivetran_synced              AS _fivetran_synced

    FROM source

)

SELECT *
FROM renamed
