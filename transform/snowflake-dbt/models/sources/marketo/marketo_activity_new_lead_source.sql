WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_new_lead') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      modifying_user                    AS modifying_user,
      created_date                      AS created_date,
      api_method_name                   AS api_method_name,
      source_type                       AS source_type,
      request_id                        AS request_id,
      _fivetran_synced                  AS _fivetran_synced,
      form_name                         AS form_name,
      lead_source                       AS lead_source,
      sfdc_type                         AS sfdc_type,
      list_name                         AS list_name

    FROM source

)

SELECT *
FROM renamed
