WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_emails_dedupe_source') }}

), renamed AS (

    SELECT
      confirmation_sent_at  AS confirmation_sent_at,
      created_at            AS created_at,
      email                 AS email_address,
      confirmed_at          AS email_confirmed_at,
      id                    AS gitlab_email_id,
      user_id               AS user_id,
      updated_at            AS updated_at
    FROM source

)

SELECT *
FROM renamed
