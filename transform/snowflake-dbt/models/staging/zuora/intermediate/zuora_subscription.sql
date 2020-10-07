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
  -- See issue https://gitlab.com/gitlab-data/analytics/-/issues/6518
  AND subscription_id NOT IN ('2c92a00774ddaf190174de37f0eb147d')
