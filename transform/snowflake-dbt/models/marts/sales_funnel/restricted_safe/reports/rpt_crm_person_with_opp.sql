{{ simple_cte([
    ('dim_crm_account','dim_crm_account'),
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('rpt_sdr_ramp_daily','rpt_sdr_ramp_daily'),
    ('mart_crm_person','mart_crm_person'),
    ('dim_crm_user','dim_crm_user')
    
]) }}

, upa_base AS (
  
    SELECT 
      dim_parent_crm_account_id,
      dim_crm_account_id
    FROM dim_crm_account
  
  ), first_order_opps AS (

    SELECT *
    FROM mart_crm_opportunity
    WHERE is_new_logo_first_order = true
    
  ), accounts_with_first_order_opps AS (

    SELECT
      upa_base.dim_parent_crm_account_id,
      upa_base.dim_crm_account_id,
      first_order_opps.dim_crm_opportunity_id,
      False AS is_first_order_available
    FROM upa_base 
    LEFT JOIN first_order_opps ON
    upa_base.dim_crm_account_id=first_order_opps.dim_crm_account_id
    WHERE dim_crm_opportunity_id IS NOT null
      
  ), final AS (

    SELECT 
      mart_crm_person.*,
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.created_date AS opportunity_created_date,
      mart_crm_opportunity.sales_accepted_date,
      mart_crm_opportunity.close_date,
      mart_crm_opportunity.sales_qualified_source_name, 
      mart_crm_opportunity.is_won,
      mart_crm_opportunity.net_arr,
      mart_crm_opportunity.is_edu_oss,
      mart_crm_opportunity.stage_name,
      mart_crm_opportunity.is_sao,
      CASE WHEN dim_crm_user.crm_user_sales_segment = 'Other' THEN rpt_sdr_ramp_daily.sdr_segment
          ELSE dim_crm_user.crm_user_sales_segment
      END AS user_sales_segment,
      CASE WHEN is_first_order_available = False THEN mart_crm_opportunity.order_type
          ELSE '1. New - First Order'
      END AS person_order_type,
      dim_crm_user.crm_user_region,
      dim_crm_user.crm_user_area,
      dim_crm_user.crm_user_geo
    FROM mart_crm_person
    LEFT JOIN mart_crm_opportunity ON
    mart_crm_person.dim_crm_account_id=mart_crm_opportunity.dim_crm_account_id
    LEFT JOIN dim_crm_user ON
    mart_crm_person.dim_crm_user_id=dim_crm_user.dim_crm_user_id
    LEFT JOIN rpt_sdr_ramp_daily ON
    mart_crm_person.dim_crm_user_id=rpt_sdr_ramp_daily.dim_crm_user_id
    LEFT JOIN upa_base ON 
    mart_crm_person.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps ON
    upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-01-20",
    updated_date="2022-01-20",
  ) }}

