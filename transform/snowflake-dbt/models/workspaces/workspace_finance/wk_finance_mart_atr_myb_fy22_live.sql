WITH dim_billing_account AS (

  SELECT *
  FROM {{ ref('dim_billing_account') }}

), dim_crm_account AS (

  SELECT *
  FROM {{ ref('dim_crm_account') }}

), dim_crm_opportunity AS (

  SELECT *
  FROM {{ ref('dim_crm_opportunity') }}

), dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

), dim_quote AS (

  SELECT *
  FROM {{ ref('dim_quote') }}

), dim_subscription AS (

  SELECT *
  FROM {{ ref('dim_subscription') }}
  WHERE subscription_status NOT IN ('Expired', 'Draft')

), fct_charge AS (

  SELECT *
  FROM {{ ref('fct_charge') }}

), fct_quote_item AS (

  SELECT *
  FROM {{ ref('fct_quote_item') }}

), zuora_subscription_snapshots AS (

    SELECT *
    FROM {{ ref('zuora_subscription_snapshots_source') }}
    WHERE subscription_status NOT IN ('Draft', 'Expired')
      AND is_deleted = FALSE
      AND exclude_from_analysis IN ('False', '')

), zuora_subscription_snapshots_spined AS (

    SELECT
      dim_date.date_id                  AS snapshot_id,
      zuora_subscription_snapshots.*
    FROM zuora_subscription_snapshots
    INNER JOIN dim_date
      ON dim_date.date_actual >= zuora_subscription_snapshots.dbt_valid_from
      AND dim_date.date_actual < {{ coalesce_to_infinity('zuora_subscription_snapshots.dbt_valid_to') }}
      AND dim_date.date_actual = '2021-01-31'
    QUALIFY rank() OVER (
         PARTITION BY subscription_name, dim_date.date_actual
         ORDER BY dbt_valid_from DESC ) = 1

), opportunity AS (

    SELECT DISTINCT
      dim_crm_opportunity.dim_crm_opportunity_id,
      fct_quote_item.dim_subscription_id
    FROM fct_quote_item
    INNER JOIN dim_crm_opportunity
      ON fct_quote_item.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    INNER JOIN dim_quote
      ON fct_quote_item.dim_quote_id = dim_quote.dim_quote_id
    WHERE stage_name IN ('Closed Won', '8-Closed Lost')
      AND is_primary_quote = TRUE

), renewal_subscriptions AS (

   SELECT DISTINCT
     sub_1.subscription_name,
     sub_1.zuora_renewal_subscription_name,
     DATE_TRUNC('month',sub_2.subscription_end_date) AS subscription_end_month
   FROM dim_subscription sub_1
   INNER JOIN dim_subscription sub_2
     ON sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
     AND DATE_TRUNC('month',sub_2.subscription_end_date) >= '2022-02-01'
   WHERE sub_1.zuora_renewal_subscription_name != ''

), base AS (--get the base data set of recurring charges.

   SELECT
     fct_charge.charge_id,
     dim_billing_account.dim_crm_account_id,
     dim_billing_account.dim_billing_account_id,
     opportunity.dim_crm_opportunity_id,
     dim_subscription.dim_subscription_id,
     fct_charge.dim_product_detail_id,
     dim_crm_account.parent_crm_account_name,
     dim_crm_account.crm_account_name,
     dim_crm_account.parent_crm_account_sales_segment,
     fct_charge.product_category,
     fct_charge.delivery,
     dim_subscription.subscription_name,
     dim_subscription.zuora_renewal_subscription_name,
     dim_subscription.current_term,
     CASE
       WHEN dim_subscription.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions) THEN TRUE
       ELSE FALSE
     END                                                                                  AS is_myb_with_multi_subs,
     fct_charge.effective_start_month,
     fct_charge.effective_end_month,
     DATE_TRUNC('month',zuora_subscription_snapshots_spined.subscription_end_date::DATE)  AS subscription_end_month,
     DATEDIFF(month,fct_charge.effective_start_month,fct_charge.effective_end_month)      AS charge_term,
     SUM(fct_charge.mrr*12)                                                               AS arr
   FROM dim_billing_account
   INNER JOIN dim_subscription
     ON dim_billing_account.dim_billing_account_id = dim_subscription.dim_billing_account_id
   INNER JOIN fct_charge
     ON dim_subscription.dim_subscription_id = fct_charge.dim_subscription_id
   LEFT JOIN opportunity
     ON dim_subscription.dim_subscription_id = opportunity.dim_subscription_id
   LEFT JOIN dim_crm_account
     ON dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
   LEFT JOIN renewal_subscriptions
     ON dim_subscription.subscription_name = renewal_subscriptions.subscription_name
   LEFT JOIN zuora_subscription_snapshots_spined
     ON dim_subscription.subscription_name = zuora_subscription_snapshots_spined.subscription_name
   WHERE fct_charge.charge_type = 'Recurring'
     AND fct_charge.mrr != 0
     AND fct_charge.effective_start_month <= '2021-01-01'
     AND fct_charge.effective_end_month > '2021-01-01'
   {{ dbt_utils.group_by(n=19) }}

), agg_charge_term_less_than_equal_12 AS (--get the starting and ending month ARR for charges with charge terms <= 12 months. These charges do not need additional logic.

  SELECT
    CASE
      WHEN is_myb_with_multi_subs = TRUE THEN 'MYB'
      ELSE 'Non-MYB'
    END                             AS renewal_type,
    is_myb_with_multi_subs,
    current_term,
    charge_term,
    charge_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_crm_opportunity_id,
    dim_subscription_id,
    dim_product_detail_id,
    product_category,
    delivery,
    subscription_name,
    effective_start_month,
    effective_end_month,
    subscription_end_month,
    SUM(arr)   AS arr
  FROM base
  WHERE charge_term <= 12
  {{ dbt_utils.group_by(n=16) }}

), agg_charge_term_greater_than_12 AS (--get the starting and ending month ARR for charges with charge terms > 12 months. These charges need additional logic.

  SELECT
    CASE
      WHEN is_myb_with_multi_subs = TRUE THEN 'MYB'
      ELSE 'Non-MYB'
    END                           AS renewal_type,
    is_myb_with_multi_subs,
    current_term,
    CASE--the below odd term charges do not behave well in the MYB logic and end up with duplicate renewals in the fiscal year. This CASE statement smooths out the charges so they only have one renewal entry in the fiscal year.
      WHEN charge_term = 26 THEN 24
      WHEN charge_term = 28 THEN 24
      WHEN charge_term = 38 THEN 36
      WHEN charge_term = 57 THEN 60
      ELSE charge_term
    END                           AS charge_term,
    charge_id,
    dim_crm_account_id,
    dim_billing_account_id,
    dim_crm_opportunity_id,
    dim_subscription_id,
    dim_product_detail_id,
    product_category,
    delivery,
    subscription_name,
    effective_start_month,
    effective_end_month,
    subscription_end_month,
    SUM(arr)   AS arr
  FROM base
  WHERE charge_term > 12
  {{ dbt_utils.group_by(n=16) }}

), twenty_four_mth_term AS (--create records for the intermitent renewals for multi-year charges that are not in the Zuora data. The start and end months are in the agg_myb for MYB.

   SELECT renewal_type, is_myb_with_multi_subs, current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id,product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/2,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 13 AND 24 AND effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=16) }}

), thirty_six_mth_term AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

   SELECT renewal_type,is_myb_with_multi_subs, current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id,product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/3,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 25 AND 36 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   UNION ALL
   SELECT renewal_type,is_myb_with_multi_subs, current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id,product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/3*2,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 25 AND 36 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   ORDER BY 1

), forty_eight_mth_term AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

   SELECT renewal_type,is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/4,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 37 AND 48 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   UNION ALL
   SELECT renewal_type,is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/4*2,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 37 AND 48 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   UNION ALL
   SELECT renewal_type,is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/4*3,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 37 AND 48 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   ORDER BY 1

), sixty_mth_term AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

   SELECT renewal_type,is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/5,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   UNION ALL
   SELECT renewal_type,is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/5*2,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   UNION ALL
   SELECT renewal_type,is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/5*3,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01'  {{ dbt_utils.group_by(n=16) }}
   UNION ALL
   SELECT renewal_type,is_myb_with_multi_subs,current_term, charge_term, charge_id, dim_crm_account_id,dim_billing_account_id,dim_crm_opportunity_id,dim_subscription_id,dim_product_detail_id, product_category,delivery,subscription_name, effective_start_month, DATEADD('month',charge_term/5*4,effective_start_month) AS effective_end_month,subscription_end_month,SUM(arr) AS arr
   FROM agg_charge_term_greater_than_12 WHERE charge_term BETWEEN 49 AND 60 AND effective_end_month > '2022-01-01' {{ dbt_utils.group_by(n=16) }}
   ORDER BY 1

), combined AS (--union all of the charges

   SELECT * FROM agg_charge_term_less_than_equal_12
   UNION ALL
   SELECT * FROM agg_charge_term_greater_than_12
   UNION ALL
   SELECT * FROM twenty_four_mth_term
   UNION ALL
   SELECT * FROM thirty_six_mth_term
   UNION ALL
   SELECT * FROM forty_eight_mth_term
   UNION ALL
   SELECT * FROM sixty_mth_term

), renewal_report AS (--create the renewal report for the applicable fiscal year.

    SELECT
      fiscal_quarter_name_fy,
      charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_crm_opportunity_id,
      dim_subscription_id,
      dim_product_detail_id,
      subscription_name,
      effective_start_month,
      effective_end_month,
      subscription_end_month,
      product_category,
      delivery,
      renewal_type,
      CASE
        WHEN subscription_end_month BETWEEN '2021-02-01' AND '2022-01-01' AND is_myb_with_multi_subs = FALSE THEN TRUE
        ELSE FALSE
      END                       AS is_atr,
      is_myb_with_multi_subs,
      current_term              AS subscription_term,
      charge_term,
      arr
    FROM combined
    LEFT JOIN dim_date
      ON combined.effective_end_month = dim_date.first_day_of_month
    WHERE effective_end_month BETWEEN '2021-02-01' AND '2022-01-01'
      AND day_of_month = 1
    ORDER BY fiscal_quarter_name_fy

)

{{ dbt_audit(
    cte_ref="renewal_report",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-03-15",
    updated_date="2021-03-15"
) }}
