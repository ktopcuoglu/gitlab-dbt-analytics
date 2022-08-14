{{ simple_cte([
    ('opportunity_base','mart_crm_opportunity'),
    ('person_base','mart_crm_person'),
    ('dim_crm_person','dim_crm_person'),
    ('mart_crm_opportunity_stamped_hierarchy_hist','mart_crm_opportunity_stamped_hierarchy_hist'),
    ('rpt_sfdc_bizible_tp_opp_linear_blended','rpt_sfdc_bizible_tp_opp_linear_blended'),
    ('dim_crm_account','dim_crm_account')
]) }}

, upa_base AS ( --pulls every account and it's UPA
  
    SELECT 
      dim_parent_crm_account_id,
      dim_crm_account_id
    FROM dim_crm_account

), first_order_opps AS ( -- pulls only FO CW Opps and their UPA/Account ID

    SELECT
      dim_parent_crm_account_id,
      dim_crm_account_id,
      dim_crm_opportunity_id,
      close_date,
      is_sao,
      sales_accepted_date
    FROM opportunity_base
    WHERE is_won = true
      AND order_type = '1. New - First Order'

), accounts_with_first_order_opps AS ( -- shows only UPA/Account with a FO Available Opp on it

    SELECT
      upa_base.dim_parent_crm_account_id,
      upa_base.dim_crm_account_id,
      first_order_opps.dim_crm_opportunity_id,
      FALSE AS is_first_order_available
    FROM upa_base 
    LEFT JOIN first_order_opps
      ON upa_base.dim_crm_account_id=first_order_opps.dim_crm_account_id
    WHERE dim_crm_opportunity_id IS NOT NULL

), person_order_type_base AS (

    SELECT DISTINCT
      person_base.email_hash, 
      person_base.dim_crm_account_id,
      upa_base.dim_parent_crm_account_id,
      opportunity_base.dim_crm_opportunity_id,
      CASE 
         WHEN is_first_order_available = False AND opportunity_base.order_type = '1. New - First Order' THEN '3. Growth'
         WHEN is_first_order_available = False AND opportunity_base.order_type != '1. New - First Order' THEN opportunity_base.order_type
      ELSE '1. New - First Order'
      END AS person_order_type,
      ROW_NUMBER() OVER( PARTITION BY email_hash ORDER BY person_order_type) AS person_order_type_number
    FROM person_base
    FULL JOIN upa_base
      ON person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN opportunity_base
      ON upa_base.dim_parent_crm_account_id=opportunity_base.dim_parent_crm_account_id

), person_order_type_final AS (

    SELECT DISTINCT
      email_hash,
      dim_crm_opportunity_id,
      dim_parent_crm_account_id,
      dim_crm_account_id,
      person_order_type
    FROM person_order_type_base
    WHERE person_order_type_number=1

), cohort_base AS (

    SELECT DISTINCT
      person_base.email_hash,
      person_base.email_domain_type,
      person_base.true_inquiry_date,
      person_base.mql_date_lastest_pt,
      person_base.status,
      person_base.lead_source,
      person_base.dim_crm_person_id,
      person_base.dim_crm_account_id,
      person_base.is_mql,
      dim_crm_person.sfdc_record_id,
      person_base.account_demographics_sales_segment,
      person_base.account_demographics_region,
      person_base.account_demographics_geo,
      person_base.account_demographics_area,
      person_base.account_demographics_upa_country,
      person_base.account_demographics_territory,
      is_first_order_available,
      person_order_type_final.person_order_type,
      opp.order_type AS opp_order_type,
      opp.sales_qualified_source_name,
      opp.deal_path_name,
      opp.sales_type,
      opp.dim_crm_opportunity_id,
      opp.sales_accepted_date,
      opp.created_date AS opp_created_date,
      opp.close_date,
      opp.is_won,
      opp.is_sao,
      opp.new_logo_count,
      opp.net_arr,
      opp.is_net_arr_closed_deal,
      opp.crm_opp_owner_sales_segment_stamped,
      opp.crm_opp_owner_region_stamped,
      opp.crm_opp_owner_area_stamped,
      opp.crm_opp_owner_geo_stamped,
      opp.parent_crm_account_demographics_upa_country,
      opp.parent_crm_account_demographics_territory
    FROM person_base
    INNER JOIN dim_crm_person
      ON person_base.dim_crm_person_id=dim_crm_person.dim_crm_person_id
    LEFT JOIN upa_base
    ON person_base.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id
    FULL JOIN mart_crm_opportunity_stamped_hierarchy_hist opp
      ON upa_base.dim_parent_crm_account_id=opp.dim_parent_crm_account_id
    LEFT JOIN person_order_type_final
      ON person_base.email_hash=person_order_type_final.email_hash

), fo_inquiry_with_tp AS (
  
  SELECT DISTINCT
  
    --Key IDs
    cohort_base.email_hash,
    cohort_base.dim_crm_person_id,
    cohort_base.dim_crm_opportunity_id,
    rpt_sfdc_bizible_tp_opp_linear_blended.dim_crm_touchpoint_id,
    cohort_base.sfdc_record_id,
  
    --person data
    CASE 
      WHEN cohort_base.person_order_type IS null AND cohort_base.opp_order_type IS null THEN 'Missing order_type_name'
      WHEN cohort_base.person_order_type IS null THEN cohort_base.opp_order_type
      ELSE person_order_type
    END AS person_order_type,
    cohort_base.lead_source,    
    cohort_base.email_domain_type,
    cohort_base.is_mql,
    cohort_base.account_demographics_sales_segment,
    cohort_base.account_demographics_geo,
    cohort_base.account_demographics_region,
    cohort_base.account_demographics_area,
    cohort_base.account_demographics_upa_country,
    cohort_base.account_demographics_territory,
    cohort_base.true_inquiry_date,
    cohort_base.mql_date_lastest_pt,
  
    --opportunity data
    cohort_base.opp_created_date,
    cohort_base.sales_accepted_date,
    cohort_base.close_date,
    cohort_base.is_sao,
    cohort_base.is_won,
    cohort_base.new_logo_count,
    cohort_base.net_arr,
    cohort_base.is_net_arr_closed_deal,
    cohort_base.opp_order_type,
    cohort_base.sales_qualified_source_name,
    cohort_base.deal_path_name,
    cohort_base.sales_type,
    cohort_base.crm_opp_owner_geo_stamped,
    cohort_base.crm_opp_owner_sales_segment_stamped,
    cohort_base.crm_opp_owner_region_stamped,
    cohort_base.crm_opp_owner_area_stamped,
    cohort_base.parent_crm_account_demographics_upa_country,
    cohort_base.parent_crm_account_demographics_territory,
    CASE
      WHEN rpt_sfdc_bizible_tp_opp_linear_blended.dim_crm_touchpoint_id IS NOT null THEN cohort_base.dim_crm_opportunity_id
      ELSE null
    END AS influenced_opportunity_id,
  
    --touchpoint data
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_touchpoint_date_normalized,
    rpt_sfdc_bizible_tp_opp_linear_blended.gtm_motion,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_integrated_campaign_grouping,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_ad_campaign_name,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_form_url,
    rpt_sfdc_bizible_tp_opp_linear_blended.bizible_landing_page,
    rpt_sfdc_bizible_tp_opp_linear_blended.is_dg_influenced,
    rpt_sfdc_bizible_tp_opp_linear_blended.is_fmm_influenced,
    rpt_sfdc_bizible_tp_opp_linear_blended.mql_sum,
    rpt_sfdc_bizible_tp_opp_linear_blended.inquiry_sum,
    rpt_sfdc_bizible_tp_opp_linear_blended.accepted_sum,
    rpt_sfdc_bizible_tp_opp_linear_blended.linear_opp_created,
    rpt_sfdc_bizible_tp_opp_linear_blended.linear_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.linear_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_linear_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_linear,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_linear_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.w_shaped_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_w_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_w,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_w_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.u_shaped_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_u_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_u,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_u_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.first_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_first_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_first,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_first_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.custom_sao,
    rpt_sfdc_bizible_tp_opp_linear_blended.pipeline_custom_net_arr,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_custom,
    rpt_sfdc_bizible_tp_opp_linear_blended.won_custom_net_arr
  FROM cohort_base
  LEFT JOIN rpt_sfdc_bizible_tp_opp_linear_blended
    ON rpt_sfdc_bizible_tp_opp_linear_blended.email_hash=cohort_base.email_hash

), final AS (

    SELECT DISTINCT *
    FROM fo_inquiry_with_tp

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-07-20",
    updated_date="2022-08-09",
  ) }}
