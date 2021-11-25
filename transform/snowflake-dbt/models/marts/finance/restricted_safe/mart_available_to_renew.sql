{{config({
    "materialized": "table",
    "transient": false
  })
}}

{% set renewal_fiscal_years= ['2019',
                              '2020',
                              '2021',
                              '2022',
                              '2023',
                              '2024',
                              '2025',
                              '2026'] %}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user','dim_crm_user')
]) }}

, dim_subscription_source AS (
   
    SELECT
      dim_subscription.*,
       CASE 
         WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
             = term_start_month THEN TRUE
         ELSE FALSE
       END AS is_dup_term
     FROM {{ ref('dim_subscription') }}
     WHERE 
       --data quality, last version is expired with no ARR in mart_arr. Should filter it out completely.
       subscription_name != 'VMware - 4000 EEP'
       --test subscription
       AND subscription_name != 'Test- New Subscription'
       --data quality, last term not entered with same pattern, sub_name = A-S00022101
       AND dim_subscription_id != '2c92a00f7579c362017588a2de19174a'
       --term dates do not align to the subscription term dates, sub_name = A-S00038937
       AND dim_subscription_id != '2c92a01177472c5201774af57f834a43'
   
), dim_subscription_int AS (

    SELECT 
      dim_subscription_source.*,
      CASE 
        WHEN LEAD(term_end_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            = term_end_month THEN TRUE
        WHEN LEAD(term_end_month,2) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            = term_end_month THEN TRUE
        WHEN LEAD(subscription_end_fiscal_year) OVER (PARTITION BY subscription_name ORDER BY 
            subscription_version) = subscription_end_fiscal_year THEN TRUE
        WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            = term_start_month THEN TRUE
        --check for subsequent subscriptiptions that are backed out
        WHEN LEAD(term_start_month) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,2) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,3) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,4) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,5) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,6) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,7) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        WHEN LEAD(term_start_month,8) OVER (PARTITION BY subscription_name ORDER BY subscription_version) 
            < term_start_month THEN TRUE
        ELSE FALSE
      END AS exclude_from_term_sorting
    FROM dim_subscription_source
    WHERE is_dup_term = FALSE

), base_subscriptions AS (
  
    SELECT 
      dim_subscription_id,
      RANK() OVER (PARTITION BY subscription_name, term_start_month ORDER BY subscription_version DESC) AS last_term_version
    FROM dim_subscription_int
    WHERE exclude_from_term_sorting = FALSE
  
), dim_subscription AS (

    SELECT dim_subscription.*
    FROM {{ ref('dim_subscription') }}
    INNER JOIN base_subscriptions
      ON dim_subscription.dim_subscription_id = base_subscriptions.dim_subscription_id
    WHERE last_term_version = 1

), mart_charge AS (

    SELECT mart_charge.*
    FROM {{ ref('mart_charge') }}
    INNER JOIN dim_subscription
      ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    WHERE is_included_in_arr_calc = 'TRUE'
      AND mart_charge.term_end_month = mart_charge.effective_end_month
      AND arr != 0
   
{% for renewal_fiscal_year in renewal_fiscal_years -%} 
), renewal_subscriptions_{{renewal_fiscal_year}} AS (

    SELECT DISTINCT
      sub_1.subscription_name,
      sub_1.zuora_renewal_subscription_name,
      DATE_TRUNC('month',sub_2.subscription_end_date) AS subscription_end_month,
      RANK() OVER (PARTITION BY sub_1.subscription_name ORDER BY sub_2.subscription_end_date DESC) AS rank
    FROM dim_subscription sub_1
    INNER JOIN dim_subscription sub_2
      ON sub_1.zuora_renewal_subscription_name = sub_2.subscription_name
      AND DATE_TRUNC('month',sub_2.subscription_end_date) > CONCAT('{{renewal_fiscal_year}}','-01-01')
    WHERE sub_1.zuora_renewal_subscription_name != ''
    QUALIFY rank = 1

), base_{{renewal_fiscal_year}} AS (--get the base data set of recurring charges.

    SELECT
      mart_charge.dim_charge_id,
      mart_charge.dim_crm_account_id,
      mart_charge.dim_billing_account_id,
      mart_charge.dim_subscription_id,
      mart_charge.dim_product_detail_id,
      mart_charge.parent_crm_account_name,
      mart_charge.crm_account_name,
      mart_charge.parent_crm_account_sales_segment,
      dim_crm_user.dim_crm_user_id,
      dim_crm_user.user_name,
      dim_crm_user.user_role_id,
      dim_crm_user.crm_user_sales_segment,
      dim_crm_user.crm_user_geo,
      dim_crm_user.crm_user_region,
      dim_crm_user.crm_user_area,
      mart_charge.product_tier_name,
      mart_charge.product_delivery_type,
      mart_charge.subscription_name,
      dim_subscription.zuora_renewal_subscription_name,
      dim_subscription.current_term,
      CASE
        WHEN dim_subscription.current_term >= 24 
          THEN TRUE
        WHEN dim_subscription.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions_{{renewal_fiscal_year}}) 
          THEN TRUE
        ELSE FALSE
      END                                                                                                                                               AS is_myb,
      CASE
        WHEN dim_subscription.subscription_name IN (SELECT DISTINCT subscription_name FROM renewal_subscriptions_{{renewal_fiscal_year}}) 
          THEN TRUE
        ELSE FALSE
      END                                                                                                                                               AS is_myb_with_multi_subs,
      mart_charge.is_paid_in_full,
      mart_charge.estimated_total_future_billings,
      mart_charge.effective_start_month,
      mart_charge.effective_end_month,
      mart_charge.subscription_start_month,
      mart_charge.subscription_end_month,
      mart_charge.term_start_month,
      mart_charge.term_end_month,
      DATEADD('month',-1,mart_charge.term_end_month)                                                                                                    AS last_paid_month_in_term,
      renewal_subscriptions_{{renewal_fiscal_year}}.subscription_end_month                                                                              AS myb_subscription_end_month,
      DATEDIFF(month,mart_charge.effective_start_month,mart_charge.effective_end_month)                                                                 AS charge_term,
      mart_charge.arr
    FROM mart_charge
    LEFT JOIN dim_subscription
      ON mart_charge.dim_subscription_id = dim_subscription.dim_subscription_id
    LEFT JOIN dim_crm_account
      ON mart_charge.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_user
      ON dim_crm_account.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    LEFT JOIN renewal_subscriptions_{{renewal_fiscal_year}}
      ON mart_charge.subscription_name = renewal_subscriptions_{{renewal_fiscal_year}}.subscription_name
    WHERE mart_charge.term_start_month <= CONCAT('{{renewal_fiscal_year}}'-1,'-01-01')
      AND mart_charge.term_end_month > CONCAT('{{renewal_fiscal_year}}'-1,'-01-01')

), agg_charge_term_less_than_equal_12_{{renewal_fiscal_year}} AS (--get the starting and ending month ARR for charges with current terms <= 12 months. These terms do not need additional logic.

    SELECT
      CASE
        WHEN is_myb = TRUE THEN 'MYB'
        ELSE 'Non-MYB'
      END                             AS renewal_type,
      is_myb,
      is_myb_with_multi_subs,
      current_term,
      --charge_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      term_end_month,
      subscription_end_month,
      SUM(arr)   AS arr
    FROM base_{{renewal_fiscal_year}}
    WHERE current_term <= 12
    {{ dbt_utils.group_by(n=22) }}

), agg_charge_term_greater_than_12_{{renewal_fiscal_year}} AS (--get the starting and ending month ARR for terms > 12 months. These terms need additional logic.

    SELECT
      CASE
        WHEN is_myb = TRUE THEN 'MYB'
        ELSE 'Non-MYB'
      END                                   AS renewal_type,
      is_myb,
      is_myb_with_multi_subs,
      --current_term,
      CASE--the below odd term charges do not behave well in the MYB logic and end up with duplicate renewals in the fiscal year. This CASE statement smooths out the charges so they only have one renewal entry in the fiscal year.
        WHEN current_term = 25 THEN 24
        WHEN current_term = 26 THEN 24
        WHEN current_term = 27 THEN 36
        WHEN current_term = 28 THEN 36
        WHEN current_term = 29 THEN 36
        WHEN current_term = 30 THEN 36
        WHEN current_term = 31 THEN 36
        WHEN current_term = 32 THEN 36
        WHEN current_term = 35 THEN 36
        WHEN current_term = 37 THEN 36
        WHEN current_term = 38 THEN 36
        WHEN current_term = 41 THEN 36
        WHEN current_term = 42 THEN 48
        WHEN current_term = 49 THEN 48
        WHEN current_term = 57 THEN 60
        ELSE current_term
      END                                   AS current_term,
      dim_charge_id,
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name,
      term_start_month,
      term_end_month,
      subscription_end_month,
      SUM(arr)                              AS arr
    FROM base_{{renewal_fiscal_year}}
    WHERE current_term > 12
    {{ dbt_utils.group_by(n=22) }}

), twenty_four_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for multi-year charges that are not in the Zuora data. The start and end months are in the agg_myb for MYB.

    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs, 
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/2,term_start_month)  AS term_end_month,
      subscription_end_month,
      SUM(arr)                                          AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 13 AND 24 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01') 
    {{ dbt_utils.group_by(n=22) }}

), thirty_six_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs, 
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/3,term_start_month)      AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 25 AND 36 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')  
    {{ dbt_utils.group_by(n=22) }}
    
    UNION ALL
    
    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs, 
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/3*2,term_start_month)    AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 25 AND 36 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01') 
    {{ dbt_utils.group_by(n=22) }}
    ORDER BY 1

), forty_eight_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs,
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id, 
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id,
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/4,term_start_month)      AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 37 AND 48 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')  
    {{ dbt_utils.group_by(n=22) }}
    
    UNION ALL
    
    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs,
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id, 
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/4*2,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 37 AND 48 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')  
    {{ dbt_utils.group_by(n=22) }}
    
    UNION ALL
    
    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs,
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id, 
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/4*3,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 37 AND 48 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')  
    {{ dbt_utils.group_by(n=22) }}
    ORDER BY 1

), sixty_mth_term_{{renewal_fiscal_year}} AS (--create records for the intermitent renewals for MYBs that are not in the Zuora data. The start and end months are in the agg_myb for MYBs.

    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs,
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id, 
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/5,term_start_month)          AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 49 AND 60 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')  
    {{ dbt_utils.group_by(n=22) }}
    
    UNION ALL
   
    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs,
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id, 
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/5*2,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 49 AND 60 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')  
    {{ dbt_utils.group_by(n=22) }}
    
    UNION ALL
    
    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs,
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id, 
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/5*3,term_start_month)        AS term_end_month,
      subscription_end_month,
      SUM(arr)                                                  AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 49 AND 60 
      AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01')  
    {{ dbt_utils.group_by(n=22) }}
    
    UNION ALL
    
    SELECT 
      renewal_type, 
      is_myb, 
      is_myb_with_multi_subs,
      current_term, 
      dim_charge_id, 
      dim_crm_account_id,
      dim_billing_account_id,
      dim_subscription_id,
      dim_crm_user_id,
      user_name,
      user_role_id,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      dim_product_detail_id, 
      product_tier_name,
      product_delivery_type,
      subscription_name, 
      term_start_month, 
      DATEADD('month',current_term/5*4,term_start_month)    AS term_end_month,
      subscription_end_month,
      SUM(arr)                                              AS arr
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}} 
    WHERE current_term BETWEEN 49 AND 60 AND term_end_month > CONCAT('{{renewal_fiscal_year}}','-01-01') 
    {{ dbt_utils.group_by(n=22) }}
    ORDER BY 1

), combined_{{renewal_fiscal_year}} AS (--union all of the charges

    SELECT * 
    FROM agg_charge_term_less_than_equal_12_{{renewal_fiscal_year}}
    
    UNION ALL
    
    SELECT * 
    FROM agg_charge_term_greater_than_12_{{renewal_fiscal_year}}
    
    UNION ALL
    
    SELECT * 
    FROM twenty_four_mth_term_{{renewal_fiscal_year}}
    
    UNION ALL
    
    SELECT * 
    FROM thirty_six_mth_term_{{renewal_fiscal_year}}
    
    UNION ALL
    
    SELECT * 
    FROM forty_eight_mth_term_{{renewal_fiscal_year}}
    
    UNION ALL
    
    SELECT * 
    FROM sixty_mth_term_{{renewal_fiscal_year}}

), opportunity_term_group AS (
  
    SELECT
      dim_subscription.dim_subscription_id,
      dim_crm_opportunity.dim_crm_opportunity_id,
      CASE
        WHEN close_date IS NULL THEN '1951-01-01'
        ELSE DATE_TRUNC('month',close_date)
      END                                     AS close_month,
      CASE
        WHEN dim_crm_opportunity.opportunity_term = 0 
          THEN '0 Years'
        WHEN dim_crm_opportunity.opportunity_term <= 12 
          THEN '1 Year'
        WHEN dim_crm_opportunity.opportunity_term > 12 
          AND dim_crm_opportunity.opportunity_term <= 24
            THEN '2 Years'
        WHEN dim_crm_opportunity.opportunity_term > 24 
          AND dim_crm_opportunity.opportunity_term <= 36 
            THEN '3 Years'
        WHEN dim_crm_opportunity.opportunity_term > 36 
          THEN '4 Years+'
        WHEN dim_crm_opportunity.opportunity_term IS NULL 
          THEN 'No Opportunity Term' 
      END                                                                                                               AS opportunity_term_group,
      CASE
        WHEN dim_crm_opportunity.opportunity_term <= 12 
          THEN FALSE
        WHEN dim_crm_opportunity.opportunity_term > 12 
          THEN TRUE
        ELSE NULL
      END                                                                                                               AS is_myb_opportunity_term_test
    FROM {{ ref('dim_subscription') }}
    LEFT JOIN {{ ref('dim_crm_opportunity') }}
      ON dim_subscription.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN {{ ref('fct_crm_opportunity') }}
      ON dim_subscription.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id
  
), renewal_report_{{renewal_fiscal_year}} AS (--create the renewal report for the applicable fiscal year.

    SELECT
      {{ dbt_utils.surrogate_key(['CONCAT(dim_date.fiscal_quarter_name_fy, base_{{renewal_fiscal_year}}.dim_charge_id)']) }} AS primary_key,
      dim_date.fiscal_year,
      dim_date.first_day_of_fiscal_quarter,
      dim_date.fiscal_quarter_name_fy,
      opportunity_term_group.close_month,
      base_{{renewal_fiscal_year}}.dim_charge_id,
      opportunity_term_group.dim_crm_opportunity_id,
      base_{{renewal_fiscal_year}}.dim_crm_account_id,
      base_{{renewal_fiscal_year}}.dim_billing_account_id,
      base_{{renewal_fiscal_year}}.dim_subscription_id,
      base_{{renewal_fiscal_year}}.dim_product_detail_id,
      base_{{renewal_fiscal_year}}.subscription_name,
      base_{{renewal_fiscal_year}}.subscription_start_month,
      base_{{renewal_fiscal_year}}.subscription_end_month,
      base_{{renewal_fiscal_year}}.term_start_month,
      base_{{renewal_fiscal_year}}.term_end_month,
      combined_{{renewal_fiscal_year}}.term_end_month                                                                               AS bookings_term_end_month,
      base_{{renewal_fiscal_year}}.myb_subscription_end_month,
      base_{{renewal_fiscal_year}}.last_paid_month_in_term,
      base_{{renewal_fiscal_year}}.current_term,
      renewal_subscriptions_{{renewal_fiscal_year}}.zuora_renewal_subscription_name,
      renewal_subscriptions_{{renewal_fiscal_year}}.subscription_end_month                                                          AS renewal_subscription_end_month,
      base_{{renewal_fiscal_year}}.parent_crm_account_name,
      base_{{renewal_fiscal_year}}.crm_account_name,
      base_{{renewal_fiscal_year}}.parent_crm_account_sales_segment,
      base_{{renewal_fiscal_year}}.dim_crm_user_id,
      base_{{renewal_fiscal_year}}.user_name,
      base_{{renewal_fiscal_year}}.user_role_id,
      base_{{renewal_fiscal_year}}.crm_user_sales_segment,
      base_{{renewal_fiscal_year}}.crm_user_geo,
      base_{{renewal_fiscal_year}}.crm_user_region,
      base_{{renewal_fiscal_year}}.crm_user_area,
      base_{{renewal_fiscal_year}}.product_tier_name,
      base_{{renewal_fiscal_year}}.product_delivery_type,
      combined_{{renewal_fiscal_year}}.renewal_type,
      base_{{renewal_fiscal_year}}.is_myb,
      base_{{renewal_fiscal_year}}.is_myb_with_multi_subs,
      base_{{renewal_fiscal_year}}.current_term                                                                                     AS subscription_term,
      base_{{renewal_fiscal_year}}.estimated_total_future_billings,
      CASE
        WHEN base_{{renewal_fiscal_year}}.term_end_month BETWEEN DATEADD('month',1, CONCAT('{{renewal_fiscal_year}}'-1,'-01-01'))
          AND CONCAT('{{renewal_fiscal_year}}','-01-01') 
            AND base_{{renewal_fiscal_year}}.is_myb_with_multi_subs = FALSE 
            THEN TRUE
        ELSE FALSE
      END                                                                                                                           AS is_atr,
      CASE
        WHEN base_{{renewal_fiscal_year}}.term_end_month BETWEEN DATEADD('month',1, CONCAT('{{renewal_fiscal_year}}'-1,'-01-01')) 
          AND CONCAT('{{renewal_fiscal_year}}','-01-01') 
            AND opportunity_term_group.is_myb_opportunity_term_test = FALSE 
              THEN TRUE
        ELSE FALSE
      END                                                                                                                           AS is_atr_opportunity_term_test,
      CASE 
        WHEN opportunity_term_group.opportunity_term_group IS NULL 
          THEN 'No Opportunity Term' 
        ELSE opportunity_term_group.opportunity_term_group
      END                                                                                                                           AS opportunity_term_group,
      base_{{renewal_fiscal_year}}.arr
    FROM combined_{{renewal_fiscal_year}}
    LEFT JOIN dim_date
      ON combined_{{renewal_fiscal_year}}.term_end_month = dim_date.first_day_of_month
    LEFT JOIN base_{{renewal_fiscal_year}}
      ON combined_{{renewal_fiscal_year}}.dim_charge_id = base_{{renewal_fiscal_year}}.dim_charge_id
    LEFT JOIN renewal_subscriptions_{{renewal_fiscal_year}}
      ON base_{{renewal_fiscal_year}}.subscription_name = renewal_subscriptions_{{renewal_fiscal_year}}.subscription_name
    LEFT JOIN opportunity_term_group
      ON base_{{renewal_fiscal_year}}.dim_subscription_id = opportunity_term_group.dim_subscription_id
    WHERE combined_{{renewal_fiscal_year}}.term_end_month BETWEEN DATEADD('month',1, CONCAT('{{renewal_fiscal_year}}'-1,'-01-01')) 
      AND CONCAT('{{renewal_fiscal_year}}','-01-01') 
        AND day_of_month = 1
    ORDER BY fiscal_quarter_name_fy

{% endfor -%}
), unioned as (

{% for renewal_fiscal_year in renewal_fiscal_years-%} 

    SELECT * 
    FROM renewal_report_{{renewal_fiscal_year}}
    {%- if not loop.last %} UNION ALL {%- endif %}
    
{% endfor -%}

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-11-24",
    updated_date="2021-11-24"
) }}