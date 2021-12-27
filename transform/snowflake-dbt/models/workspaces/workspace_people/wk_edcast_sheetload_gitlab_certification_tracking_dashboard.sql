{{ config(
    tags=["people", "edcast"]
) }}

WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'gitlab_certification_tracking_dashboard') }}

), renamed AS (

  SELECT
    user::VARCHAR                                       AS user,
    account::VARCHAR                                    AS account,
    passed_datetime::TIMESTAMP                          AS passed_datetime,
    certification::VARCHAR                              AS certification,
    cert_date::DATE                                     AS cert_date,
    partner_sfdc_id::VARCHAR                            AS partner_sfdc_id,
    account_owner::VARCHAR                              AS account_owner,
    region::VARCHAR                                     AS region,
    track::VARCHAR                                      AS track,
    pubsec_partner::BOOLEAN                             AS pubsec_partner,
    cert_month::VARCHAR                                 AS cert_month,
    cert_quarter::VARCHAR                               AS cert_quarter,
    _updated_at::TIMESTAMP AS                           AS _updated_at
  FROM source

)

SELECT *
FROM renamed
