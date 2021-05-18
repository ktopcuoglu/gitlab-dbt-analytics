WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_in_product_marketing_emails_dedupe_source') }}
  
),
renamed AS (

    SELECT
      id::NUMBER                    AS in_product_marketing_email_id,
      user_id::NUMBER               AS user_id,
      cta_clicked_at::TIMESTAMP     AS cta_clicked_at,
      track::NUMBER                 AS track,
      series::NUMBER                AS series,
      created_at::TIMESTAMP         AS created_at,
      updated_at::TIMESTAMP         AS updated_at
    FROM source

)

SELECT *
FROM renamed
