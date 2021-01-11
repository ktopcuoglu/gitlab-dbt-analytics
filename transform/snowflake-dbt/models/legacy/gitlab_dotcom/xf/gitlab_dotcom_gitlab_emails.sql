{% set column_name = 'email_handle' %}


WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_emails_source') }}

), filtered AS (

    SELECT *,
      SPLIT_PART(email_address,'@', 0)      AS email_handle,
      {{include_gitlab_email(column_name)}} AS include_email_flg
    FROM source
    WHERE LOWER(email_address) LIKE '%@gitlab.com'

)

SELECT *
FROM filtered
