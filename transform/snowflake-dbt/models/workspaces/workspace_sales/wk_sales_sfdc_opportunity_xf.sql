{{ config(alias='sfdc_opportunity_xf') }}

WITH sfdc_opportunity_xf AS (

    SELECT * FROM {{ref('sfdc_opportunity_xf')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

), sfdc_account AS (

    SELECT * FROM {{ref('sfdc_account')}}

), date_details AS (
 
    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), oppty_final AS (

    SELECT 
      sfdc_opportunity_xf.*,
      
      -- medium level grouping of the order type field
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth') 
          THEN '2. Growth' 
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('4. Churn','4. Contraction','6. Churn - Final')
          THEN '3. Churn'
        ELSE '4. Other' 
      END                                                                 AS deal_category,

      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth', '4. Churn','4. Contraction','6. Churn - Final') 
          THEN '2. Growth' 
        ELSE '3. Other'
      END                                                                 AS deal_group,
   
      -- PIO Flag for PIO reporting dashboard
      CASE 
        WHEN (sfdc_opportunity_xf.partner_initiated_opportunity = TRUE -- up to the first half of the year 2020
            AND sfdc_opportunity_xf.created_date < '2020-08-01'::DATE)
          OR (sfdc_opportunity_xf.dr_partner_engagement = 'PIO' -- second half and moving forward  
            AND sfdc_opportunity_xf.created_date >= '2020-08-01'::DATE)
          THEN 1 
        ELSE 0 
      END                                                                 AS partner_engaged_opportunity_flag,

      CASE 
        WHEN sfdc_opportunity_xf.account_owner_team_vp_level = 'VP Ent'
          THEN 'Large'
        WHEN sfdc_opportunity_xf.account_owner_team_vp_level = 'VP Comm MM'
          THEN 'Mid-Market'
        WHEN sfdc_opportunity_xf.account_owner_team_vp_level = 'VP Comm SMB' 
          THEN 'SMB' 
        ELSE 'Other' 
      END                                                                 AS account_owner_cro_level,

      CASE 
        WHEN sfdc_opportunity_xf.user_segment   IS NULL 
          OR sfdc_opportunity_xf.user_segment   = 'Unknown' 
        THEN 'SMB' 
          ELSE sfdc_opportunity_xf.user_segment   
      END                                                                 AS user_segment_stamped,

       -- check if renewal was closed on time or not
      CASE 
        WHEN sfdc_opportunity_xf.is_renewal = 1 
          AND sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date >= sfdc_opportunity_xf.close_fiscal_quarter_date 
            THEN 'On-Time'
        WHEN sfdc_opportunity_xf.is_renewal = 1 
          AND sfdc_opportunity_xf.subscription_start_date_fiscal_quarter_date < sfdc_opportunity_xf.close_fiscal_quarter_date 
            THEN 'Late' 
      END                                                                 AS renewal_timing_status,

      --********************************************************
      -- calculated fields for pipeline velocity report
      
      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN sfdc_account.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND sfdc_opportunity_xf.close_date < '2020-08-01' 
            THEN 1
        ELSE 0
      END                                                                 AS is_excluded_flag

    FROM sfdc_opportunity_xf
    LEFT JOIN sfdc_account
      ON sfdc_account.account_id = sfdc_opportunity_xf.account_id
)
SELECT *
FROM oppty_final