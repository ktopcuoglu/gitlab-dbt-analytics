/* This table needs to be permanent to allow zero cloning at specific timestamps */
{{
  config( materialized='incremental',
    incremental_startegy='merge',
    unique_key='primary_key')
  }}

{% if execute %}
  {% if flags.FULL_REFRESH and target.name == 'prod' %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude marts.arr_data_mart_incr\".") }}
  {% endif %}
{% endif %}

WITH fct_charges AS (

    SELECT *
    FROM {{ ref('fct_charges_valid_at') }}

), fct_invoice_items_agg AS (

    SELECT *
    FROM {{ ref('fct_invoice_items_agg_valid_at') }}

), dim_crm_accounts AS (

    SELECT *
    FROM {{ ref('dim_crm_accounts_valid_at') }}

), dim_billing_accounts AS (

    SELECT *
    FROM {{ ref('dim_billing_accounts_valid_at') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions_valid_at') }}

), last_month_of_fiscal_quarter AS (

    SELECT DISTINCT
      DATE_TRUNC('month', last_day_of_fiscal_quarter) AS last_month_of_fiscal_quarter,
      fiscal_quarter_name_fy
    FROM {{ ref('dim_dates') }}

), last_month_of_fiscal_year AS (

    SELECT DISTINCT
      DATE_TRUNC('month', last_day_of_fiscal_year) AS last_month_of_fiscal_year,
      fiscal_year
    FROM {{ ref('dim_dates') }}

), base_charges AS (

    SELECT
      --date info
      fct_charges.effective_start_date_id,
      fct_charges.effective_end_date_id,
      fct_charges.effective_start_month,
      fct_charges.effective_end_month,
      dim_subscriptions.subscription_start_month,
      dim_subscriptions.subscription_end_month,

      --account info
      dim_billing_accounts.billing_account_id                                          AS zuora_account_id,
      dim_billing_accounts.sold_to_country                                             AS zuora_sold_to_country,
      dim_billing_accounts.billing_account_name                                        AS zuora_account_name,
      dim_billing_accounts.billing_account_number                                      AS zuora_account_number,
      COALESCE(dim_crm_accounts.merged_to_account_id, dim_crm_accounts.crm_account_id) AS crm_id,
      dim_crm_accounts.ultimate_parent_account_id,
      dim_crm_accounts.ultimate_parent_account_name,
      dim_crm_accounts.ultimate_parent_billing_country,
      dim_crm_accounts.ultimate_parent_account_segment,
      dim_crm_accounts.ultimate_parent_industry,
      dim_crm_accounts.ultimate_parent_account_owner_team,
      dim_crm_accounts.ultimate_parent_territory,

      --subscription info
      dim_subscriptions.subscription_id,
      dim_subscriptions.subscription_name,
      dim_subscriptions.subscription_name_slugify,
      dim_subscriptions.subscription_status,

      --charge info
      fct_charges.charge_id,
      fct_charges.product_details_id,
      fct_charges.rate_plan_charge_number,
      fct_charges.rate_plan_charge_segment,
      fct_charges.rate_plan_charge_version,
      fct_charges.rate_plan_name,
      fct_charges.product_category,
      fct_charges.delivery,
      fct_charges.service_type,
      fct_charges.charge_type,
      fct_charges.unit_of_measure,
      fct_charges.mrr,
      fct_charges.mrr*12                                                    AS arr,
      fct_charges.quantity
    FROM dim_billing_accounts
    INNER JOIN dim_subscriptions
      ON dim_billing_accounts.billing_account_id= dim_subscriptions.billing_account_id
    INNER JOIN fct_charges
      ON dim_subscriptions.subscription_id = fct_charges.subscription_id
    LEFT JOIN dim_crm_accounts
      ON dim_billing_accounts.crm_account_id = dim_crm_accounts.crm_account_id

), latest_invoiced_charge_version_in_segment AS (

    SELECT
      base_charges.charge_id,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY base_charges.rate_plan_charge_number, base_charges.rate_plan_charge_segment
          ORDER BY base_charges.rate_plan_charge_version DESC, fct_invoice_items_agg.service_start_date DESC) = 1,
          TRUE, FALSE
      ) AS is_last_segment_version
    FROM base_charges
    INNER JOIN fct_invoice_items_agg
      ON base_charges.charge_id = fct_invoice_items_agg.charge_id

), charges_agg AS (

    SELECT
      base_charges.*,
      latest_invoiced_charge_version_in_segment.is_last_segment_version
    FROM base_charges
    LEFT JOIN latest_invoiced_charge_version_in_segment
      ON base_charges.charge_id = latest_invoiced_charge_version_in_segment.charge_id

), dim_dates AS (

      SELECT *
      FROM {{ ref('dim_dates') }}

), charges_month_by_month AS (

      SELECT
        '{{ var('valid_at') }}'::DATE AS snapshot_date,
        charges_agg.*,
        dim_dates.date_actual         AS arr_month
      FROM charges_agg
      INNER JOIN dim_dates
        ON charges_agg.effective_start_month <= dim_dates.date_actual
        AND (charges_agg.effective_end_month > dim_dates.date_actual OR charges_agg.effective_end_month IS NULL)
        AND dim_dates.day_of_month = 1
      WHERE subscription_status NOT IN ('Draft', 'Expired')
        AND charges_agg.charge_type = 'Recurring'
        AND mrr != 0

  )

  SELECT
    --primary_key
    {{ dbt_utils.surrogate_key(['snapshot_date', 'arr_month', 'subscription_name', 'product_category']) }}
                                 AS primary_key,

    --date info
    snapshot_date,
    arr_month,
    quarter.fiscal_quarter_name_fy,
    year.fiscal_year,
    subscription_start_month,
    subscription_end_month,

    --account info
    zuora_account_id,
    zuora_sold_to_country,
    zuora_account_name,
    zuora_account_number,
    crm_id,
    ultimate_parent_account_id,
    ultimate_parent_account_name,
    ultimate_parent_billing_country,
    ultimate_parent_account_segment,

    --subscription info
    subscription_name,
    subscription_name_slugify,
    subscription_status,

    --charge info
    product_category,
    delivery,
    service_type,
    charge_type,
    array_agg(unit_of_measure)    AS unit_of_measure,
    array_agg(rate_plan_name)     AS rate_plan_name,
    SUM(mrr)                      AS mrr,
    SUM(arr)                      AS arr,
    SUM(quantity)                 AS quantity
  FROM charges_month_by_month
  LEFT JOIN last_month_of_fiscal_quarter quarter
    ON charges_month_by_month.arr_month = quarter.last_month_of_fiscal_quarter
  LEFT JOIN last_month_of_fiscal_year year
    ON  charges_month_by_month.arr_month = year.last_month_of_fiscal_year
  {{ dbt_utils.group_by(n=23) }}
