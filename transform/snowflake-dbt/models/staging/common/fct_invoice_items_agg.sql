WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE

), zuora_invoice AS (

    SELECT *
    FROM {{ ref('zuora_invoice_source') }}
    WHERE is_deleted = FALSE

), zuora_invoice_item AS (

    SELECT *
    FROM  {{ ref('zuora_invoice_item_source') }}
    WHERE is_deleted= FALSE

), zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}
    WHERE is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), zuora_subscription_snapshots AS (

  /**
  This partition handles duplicates and hard deletes by taking only
    the latest subscription version snapshot
   */

  SELECT
    rank() OVER (
      PARTITION BY subscription_name
      ORDER BY DBT_VALID_FROM DESC) AS rank,
    subscription_id
  FROM {{ ref('zuora_subscription_snapshots_source') }}
  WHERE subscription_status NOT IN ('Draft', 'Expired')
    AND CURRENT_TIMESTAMP()::TIMESTAMP_TZ >= dbt_valid_from
    AND {{ coalesce_to_infinity('dbt_valid_to') }} > current_timestamp()::TIMESTAMP_TZ

), base_charges AS (

    SELECT
      zuora_account.account_id                                                  AS subscription_account_id,
      zuora_subscription.subscription_id,
      zuora_rate_plan_charge.rate_plan_charge_id                                AS charge_id,
      zuora_rate_plan_charge.rate_plan_charge_number,
      zuora_rate_plan_charge.segment                                            AS rate_plan_charge_segment,
      zuora_rate_plan_charge.version                                            AS rate_plan_charge_version,
      zuora_rate_plan_charge.mrr,
      zuora_rate_plan_charge.mrr*12                                             AS arr,
      zuora_rate_plan_charge.quantity,
      DATE_TRUNC('month',zuora_rate_plan_charge.effective_start_date::DATE)     AS effective_start_month,
      DATE_TRUNC('month',zuora_rate_plan_charge.effective_end_date::DATE)       AS effective_end_month
    FROM zuora_rate_plan_charge
    INNER JOIN zuora_rate_plan
      ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id
    INNER JOIN zuora_subscription
      ON zuora_rate_plan.subscription_id = zuora_subscription.subscription_id
    INNER JOIN zuora_subscription_snapshots
      ON zuora_subscription_snapshots.subscription_id = zuora_subscription.subscription_id
      AND zuora_subscription_snapshots.rank = 1
    INNER JOIN zuora_account
      ON zuora_account.account_id = zuora_subscription.account_id
    WHERE zuora_subscription.subscription_status NOT IN ('Expired', 'Draft')

), invoice_charges AS (

    SELECT
      zuora_invoice.invoice_number,
      zuora_invoice.invoice_date::DATE                  AS invoice_date,
      zuora_invoice.account_id                          AS invoice_account_id,
      zuora_invoice_item.rate_plan_charge_id            AS charge_id,
      zuora_invoice_item.product_rate_plan_charge_id    AS product_details_id,
      zuora_invoice_item.sku                            AS sku,
      SUM(zuora_invoice_item.charge_amount)             AS charge_amount_sum,
      SUM(zuora_invoice_item.tax_amount)                AS tax_amount_sum
    FROM zuora_invoice_item
    INNER JOIN zuora_invoice
      ON zuora_invoice_item.invoice_id = zuora_invoice.invoice_id
    WHERE zuora_invoice.status='Posted'
    {{ dbt_utils.group_by(n=6) }}

), is_last_segment_version AS (

    SELECT
      base_charges.charge_id,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY rate_plan_charge_number, rate_plan_charge_segment
          ORDER BY rate_plan_charge_version DESC) = 1,
          TRUE, FALSE
      ) AS is_last_segment_version
    FROM base_charges
    INNER JOIN invoice_charges
      ON base_charges.charge_id = invoice_charges.charge_id

)

SELECT
  base_charges.subscription_account_id,
  invoice_charges.invoice_account_id,
  base_charges.subscription_id,
  invoice_charges.charge_id,
  invoice_charges.product_details_id,
  invoice_charges.invoice_number,
  invoice_charges.invoice_date,
  base_charges.effective_start_month,
  base_charges.effective_end_month,
  base_charges.quantity,
  base_charges.mrr,
  base_charges.arr,
  invoice_charges.charge_amount_sum,
  invoice_charges.tax_amount_sum,
  is_last_segment_version.is_last_segment_version
FROM base_charges
INNER JOIN invoice_charges
  ON base_charges.charge_id = invoice_charges.charge_id
LEFT JOIN is_last_segment_version
  ON base_charges.charge_id = is_last_segment_version.charge_id
