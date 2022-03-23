WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_change_status_in_progression') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      old_status_id                     AS old_status_id,
      new_status_id                     AS new_status_id,
      acquired_by                       AS acquired_by,
      old_status                        AS old_status,
      new_status                        AS new_status,
      program_member_id                 AS program_member_id,
      success                           AS success,
      registration_code                 AS registration_code,
      webinar_url                       AS webinar_url,
      reason                            AS reason,
      reached_success_date              AS reached_success_date

    FROM source

)

SELECT *
FROM renamed
