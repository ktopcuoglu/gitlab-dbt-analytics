WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'lead') }}

), renamed AS (

    SELECT
      --Primary Key
      id::FLOAT                                 AS marketo_lead_id,

      --Info
      email::VARCHAR                            AS email,
      first_name::VARCHAR                       AS first_name,
      last_name::VARCHAR                        AS last_name,
      company::VARCHAR                          AS company_name,
      title::VARCHAR                            AS job_title,
      {{it_job_title_hierarchy('job_title')}},
      country::VARCHAR                          AS country,
      mobile_phone::VARCHAR                     AS mobile_phone,
      inactive_lead_c::BOOLEAN                  AS is_lead_inactive,
      inactive_contact_c::BOOLEAN               AS is_contact_inactive,
      sales_segmentation_c::VARCHAR             AS sales_segmentation,
      is_email_bounced::BOOLEAN                 AS is_email_bounced,
      email_bounced_date::DATE                  AS email_bounced_date,
      unsubscribed::BOOLEAN                     AS is_unsubscribed,
      compliance_segment_value::VARCHAR         AS compliance_segment_value,
      updated_at::TIMESTAMP                     AS updated_at

    FROM source
    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 1

)

SELECT *
FROM renamed
