{{ config({
        "materialized": "incremental",
        "unique_key": "mrr_snapshot_id",
        "tags": ["arr_snapshots"],
        "schema": "workspace_finance"
    })
}}

/* grain: one record per subscription, product charge per month */
WITH dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' AND date_actual <= CURRENT_DATE

   {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

   {% endif %}

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_snapshots_source') }}
    WHERE is_deleted = FALSE

), zuora_account_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_account.*
    FROM zuora_account
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_account.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_account.dbt_valid_to') }}

), zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_snapshots_source') }}

), zuora_rate_plan_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_rate_plan.*
    FROM zuora_rate_plan
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_rate_plan.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_rate_plan.dbt_valid_to') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_snapshots_source') }}
    WHERE charge_type = 'Recurring'
      AND mrr != 0 /* This excludes Education customers (charge name EDU or OSS) with free subscriptions */

), zuora_rate_plan_charge_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_rate_plan_charge.*
    FROM zuora_rate_plan_charge
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_rate_plan_charge.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_rate_plan_charge.dbt_valid_to') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_snapshots_source') }}
    WHERE subscription_status NOT IN ('Draft', 'Expired')
       AND is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), zuora_subscription_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      zuora_subscription.*
    FROM zuora_subscription
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= zuora_subscription.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('zuora_subscription.dbt_valid_to') }}
    QUALIFY rank() OVER (
         PARTITION BY subscription_name, snapshot_dates.date_actual
         ORDER BY dbt_valid_from DESC) = 1

), rate_plan_charge_filtered AS (

    SELECT
      zuora_rate_plan_charge_spined.snapshot_id,
      zuora_account_spined.account_id                                           AS dim_billing_account_id,
      zuora_account_spined.crm_id                                               AS dim_crm_account_id,
      zuora_rate_plan_charge_spined.rate_plan_charge_id                         AS charge_id,
      zuora_rate_plan_charge_spined.rate_plan_charge_name,
      zuora_subscription_spined.subscription_id                                 AS dim_subscription_id,
      zuora_subscription_spined.subscription_name,
      zuora_rate_plan_charge_spined.product_rate_plan_charge_id                 AS dim_product_details_id,
      zuora_rate_plan_charge_spined.mrr,
      zuora_rate_plan_charge_spined.delta_mrc                                   AS delta_mrr,
      zuora_rate_plan_charge_spined.unit_of_measure,
      zuora_rate_plan_charge_spined.quantity,
      DATE_TRUNC('month', zuora_subscription_spined.subscription_start_date)    AS subscription_start_month,
      DATE_TRUNC('month', zuora_subscription_spined.subscription_end_date)      AS subscription_end_month,
      zuora_subscription_spined.subscription_start_date,
      zuora_subscription_spined.subscription_end_date,
      zuora_rate_plan_charge_spined.effective_start_month,
      zuora_rate_plan_charge_spined.effective_end_month,
      zuora_rate_plan_charge_spined.effective_start_date,
      zuora_rate_plan_charge_spined.effective_end_date
    FROM zuora_rate_plan_charge_spined
    INNER JOIN zuora_rate_plan_spined
      ON zuora_rate_plan_spined.rate_plan_id = zuora_rate_plan_charge_spined.rate_plan_id
        AND zuora_rate_plan_spined.snapshot_id = zuora_rate_plan_charge_spined.snapshot_id
    INNER JOIN zuora_subscription_spined
      ON zuora_rate_plan_spined.subscription_id = zuora_subscription_spined.subscription_id
        AND zuora_rate_plan_spined.snapshot_id = zuora_subscription_spined.snapshot_id
    INNER JOIN zuora_account_spined
      ON zuora_account_spined.account_id = zuora_subscription_spined.account_id
        AND zuora_account_spined.snapshot_id = zuora_subscription_spined.snapshot_id

), mrr_day_by_day AS (

    SELECT
      dim_date.date_id,
      dim_date.day_actual                                  AS snapshot_date,
      snapshot_id,
      dim_billing_account_id,
      dim_crm_account_id,
      charge_id,
      dim_subscription_id,
      dim_product_details_id,
      subscription_start_month,
      subscription_end_month,
      subscription_start_date,
      subscription_end_date,
      effective_start_month,
      effective_end_month,
      effective_start_date,
      effective_end_date,
      subscription_name,
      rate_plan_charge_name,
      SUM(delta_mrr)                                       AS delta_mrr,
      SUM(mrr)                                             AS mrr,
      SUM(delta_mrr)* 12                                   AS delta_arr,
      SUM(mrr)* 12                                         AS arr,
      SUM(quantity)                                        AS quantity,
      ARRAY_AGG(rate_plan_charge_filtered.unit_of_measure) AS unit_of_measure
    FROM rate_plan_charge_filtered
    INNER JOIN dim_date
      ON rate_plan_charge_filtered.effective_start_date <= dim_date.date_actual
      AND (rate_plan_charge_filtered.effective_end_date > dim_date.date_actual
        OR rate_plan_charge_filtered.effective_end_date IS NULL)
    {{ dbt_utils.group_by(n=18) }}

), final AS (

    SELECT
      {{ dbt_utils.surrogate_key(['snapshot_id', 'date_id', 'subscription_name', 'dim_product_details_id', 'charge_id']) }}
          AS mrr_snapshot_id,
      {{ dbt_utils.surrogate_key(['date_id', 'subscription_name', 'dim_product_details_id', 'charge_id']) }}
          AS mrr_id,
      snapshot_id,
      snapshot_date,
      dim_billing_account_id,
      dim_crm_account_id,
      charge_id,
      dim_subscription_id,
      subscription_name,
      dim_product_details_id,
      subscription_start_month,
      subscription_end_month,
      subscription_start_date,
      subscription_end_date,
      effective_start_month,
      effective_end_month,
      effective_start_date,
      effective_end_date,
      mrr,
      arr,
      quantity,
      unit_of_measure
    FROM mrr_day_by_day

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-02-13",
    updated_date="2020-02-13",
 	) }}
