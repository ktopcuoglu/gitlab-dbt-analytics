WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_merge_leads') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      merge_ids                         AS merge_ids,
      merged_in_sales                   AS merged_in_sales,
      merge_source                      AS merge_source,
      master_updated                    AS master_updated

    FROM source

)

SELECT *
FROM renamed
