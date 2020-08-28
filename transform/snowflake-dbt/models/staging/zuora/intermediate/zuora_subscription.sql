-- depends_on: {{ ref('zuora_excluded_accounts') }}

WITH source AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

)

SELECT *
FROM source
WHERE is_deleted = FALSE
  AND exclude_from_analysis IN ('False', '')
  AND account_id NOT IN ({{ zuora_excluded_accounts() }})
  AND subscription_id != '2c92a008742e0c63017434b81fc44320' -- https://gitlab.com/gitlab-data/analytics/-/merge_requests/3474
