{{
    config(
        materialized='incremental'
    )
}}

WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_dates') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_snapshots_source') }}
    WHERE subscription_status NOT IN ('Draft', 'Expired')

), zuora_subscription_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_subscription.*,
      -- hard deletes will be ranked 2
      rank() OVER (
         PARTITION BY subscription_name, snapshot_dates.date_actual
         ORDER BY DBT_VALID_FROM DESC) AS rank
    FROM zuora_subscription
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_subscription.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_subscription.dbt_valid_to') }}

), zuora_account AS (

    SELECT
      account_id,
      crm_id
    FROM {{ ref('zuora_account_source') }}

), joined AS (

    SELECT
      zuora_subscription_spined.snapshot_id,
      {{ dbt_utils.surrogate_key(['zuora_subscription_spined.subscription_name']) }}
        AS subscription_id,
      zuora_subscription_spined.subscription_id                                 AS zuora_subscription_id,
      zuora_account.crm_id                                                      AS crm_account_id,
      zuora_account.account_id                                                  AS billing_account_id,
      zuora_subscription_spined.subscription_name,
      zuora_subscription_spined.subscription_name_slugify,
      zuora_subscription_spined.subscription_status,
      zuora_subscription_spined.version                                         AS subscription_version,
      zuora_subscription_spined.auto_renew                                      AS is_auto_renew,
      zuora_subscription_spined.zuora_renewal_subscription_name,
      zuora_subscription_spined.zuora_renewal_subscription_name_slugify,
      zuora_subscription_spined.renewal_term,
      zuora_subscription_spined.renewal_term_period_type,
      zuora_subscription_spined.subscription_start_date                         AS subscription_start_date,
      zuora_subscription_spined.subscription_end_date                           AS subscription_end_date,
      DATE_TRUNC('month', zuora_subscription_spined.subscription_start_date)    AS subscription_start_month,
      DATE_TRUNC('month', zuora_subscription_spined.subscription_end_date)      AS subscription_end_month
    FROM zuora_subscription_spined
    INNER JOIN zuora_account
      ON zuora_account.account_id = zuora_subscription_spined.account_id
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')
      AND zuora_subscription_spined.rank = 1


), final AS (

    SELECT
        {{ dbt_utils.surrogate_key(['snapshot_id', 'subscription_id']) }}
          AS subscription_snapshot_id,
        *
    FROM joined

    {% if is_incremental() %}

      -- this filter will only be applied on an incremental run
     WHERE snapshot_id > (select max(snapshot_id) from {{ this }})

    {% endif %}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@msendal",
    created_date="2020-09-29",
    updated_date="2020-09-29"
) }}

