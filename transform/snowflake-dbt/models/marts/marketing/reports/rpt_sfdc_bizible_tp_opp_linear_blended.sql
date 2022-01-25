{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('rpt_sfdc_bizible_linear','rpt_sfdc_bizible_linear'),
    ('rpt_pmg_data','rpt_pmg_data'),
    ('rpt_sfdc_bizible_tp_person_lifecycle','rpt_sfdc_bizible_tp_person_lifecycle'),
    ('dim_date','dim_date')
]) }}

WITH unioned AS (
  
    SELECT 
      rpt_pmg_data.reporting_date_month_yr AS bizible_touchpoint_date_month_yr,
      rpt_pmg_data.reporting_date_normalized AS bizible_touchpoint_date_normalized,
      rpt_pmg_data.integrated_campaign_grouping AS bizible_integrated_campaign_grouping,
      rpt_pmg_data.mapped_channel_source_normalized AS bizible_marketing_channel_path,
      rpt_pmg_data.region_normalized AS region_normalized, --5       
      IFF(rpt_pmg_data.utm_segment IS null,'Unknown',rpt_pmg_data.utm_segment) AS sales_segment_name,
      null AS crm_person_status,
      null AS bizible_touchpoint_type,
      null AS sales_type, 
      rpt_pmg_data.reporting_date_normalized AS opp_created_date, --10    
      rpt_pmg_data.reporting_date_normalized AS sales_accepted_date,
      rpt_pmg_data.reporting_date_normalized AS close_date,          
      null AS stage_name, 
      null AS is_won,
      null AS is_sao,
      null AS deal_path_name, -- 15
      null AS order_type, 
      null AS bizible_landing_page,
      null AS bizible_form_url,
      null AS dim_crm_account_id, 
      null AS dim_crm_opportunity_id, -- 20
      null AS crm_account_name,
      null AS crm_account_gtm_strategy,
      null AS country,
      rpt_pmg_data.mapped_channel AS bizible_medium, 
      rpt_pmg_data.touchpoint_segment, -- 25
      rpt_pmg_data.gtm_motion,
      null AS last_utm_campaign,
      null AS last_utm_content,
      null AS bizible_ad_campaign_name,
      null AS lead_source, --30
      null AS campaign_type,
      rpt_pmg_data.reporting_date_normalized AS mql_datetime_least,
      null AS true_inquiry_date,
      null AS dim_crm_person_id,
      null AS is_inquiry, 
      null AS is_mql,
      SUM(rpt_pmg_data.cost) AS total_cost,
      0 AS touchpoint_sum,
      0 AS new_lead_created_sum,
      0 AS count_true_inquiry,   
      0 AS mql_sum,
      0 AS accepted_sum,
      0 AS new_mql_sum,
      0 AS new_accepted_sum,
      0 AS first_opp_created,
      0 AS u_shaped_opp_created,
      0 AS w_shaped_opp_created,
      0 AS full_shaped_opp_created,
      0 AS custom_opp_created,
      0 AS linear_opp_created,
      0 AS first_net_arr,
      0 AS u_net_arr,
      0 AS w_net_arr,
      0 AS full_net_arr,
      0 AS custom_net_arr,
      0 AS linear_net_arr,
      0 AS first_sao,
      0 AS u_shaped_sao,
      0 AS w_shaped_sao,
      0 AS full_shaped_sao,
      0 AS custom_sao,
      0 AS linear_sao,
      0 AS pipeline_first_net_arr,
      0 AS pipeline_u_net_arr,
      0 AS pipeline_w_net_arr,
      0 AS pipeline_full_net_arr,
      0 AS pipeline_custom_net_arr,
      0 AS pipeline_linear_net_arr,
      0 AS won_first,
      0 AS won_u,
      0 AS won_w,
      0 AS won_full,
      0 AS won_custom,
      0 AS won_linear,
      0 AS won_first_net_arr,
      0 AS won_u_net_arr,
      0 AS won_w_net_arr,
      0 AS won_full_net_arr,
      0 AS won_custom_net_arr,
      0 AS won_linear_net_arr
    FROM rpt_pmg_data 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
    UNION ALL
    SELECT 
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_touchpoint_date_month_yr,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_touchpoint_date_normalized,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_integrated_campaign_grouping,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_marketing_channel_path,
      CASE 
        WHEN rpt_sfdc_bizible_tp_person_lifecycle.region = 'NORAM' THEN 'AMER'
        ELSE rpt_sfdc_bizible_tp_person_lifecycle.region 
      END AS region_normalized, -- 5
      IFF(rpt_sfdc_bizible_tp_person_lifecycle.sales_segment_name IS null,'Unknown',rpt_sfdc_bizible_tp_person_lifecycle.sales_segment_name) AS sales_segment_name,
      rpt_sfdc_bizible_tp_person_lifecycle.crm_person_status,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_touchpoint_type, 
      null AS sales_type,
      null AS opp_created_date,
      null AS sales_accepted_date,
      null AS close_date, 
      null AS stage_name,
      null AS is_won,-- 15
      null AS is_sao,
      null AS deal_path_name, 
      null AS order_type,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_landing_page,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_form_url,
      rpt_sfdc_bizible_tp_person_lifecycle.dim_crm_account_id, --20
      null AS dim_crm_opportunity_id,
      rpt_sfdc_bizible_tp_person_lifecycle.crm_account_name AS crm_account_name, 
      rpt_sfdc_bizible_tp_person_lifecycle.crm_account_gtm_strategy,
      UPPER(rpt_sfdc_bizible_tp_person_lifecycle.person_country) AS country,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_medium AS bizible_medium,
      rpt_sfdc_bizible_tp_person_lifecycle.touchpoint_segment, -- 25
      rpt_sfdc_bizible_tp_person_lifecycle.gtm_motion,
      rpt_sfdc_bizible_tp_person_lifecycle.last_utm_campaign,
      rpt_sfdc_bizible_tp_person_lifecycle.last_utm_content,
      rpt_sfdc_bizible_tp_person_lifecycle.bizible_ad_campaign_name,
      rpt_sfdc_bizible_tp_person_lifecycle.lead_source,
      rpt_sfdc_bizible_tp_person_lifecycle.campaign_type,
      rpt_sfdc_bizible_tp_person_lifecycle.mql_datetime_least::date AS mql_datetime_least, --30
      rpt_sfdc_bizible_tp_person_lifecycle.true_inquiry_date,
      rpt_sfdc_bizible_tp_person_lifecycle.dim_crm_person_id AS dim_crm_person_id,
      is_inquiry,
      is_mql,
      0 AS Total_cost,
      SUM(rpt_sfdc_bizible_tp_person_lifecycle.touchpoint_count) AS touchpoint_sum,
      SUM(rpt_sfdc_bizible_tp_person_lifecycle.bizible_count_lead_creation_touch) AS new_lead_created_sum,
      SUM(rpt_sfdc_bizible_tp_person_lifecycle.count_true_inquiry) AS count_true_inquiry, 
      SUM(rpt_sfdc_bizible_tp_person_lifecycle.count_mql) AS mql_sum,
      SUM(rpt_sfdc_bizible_tp_person_lifecycle.count_accepted) AS accepted_sum,
      SUM(rpt_sfdc_bizible_tp_person_lifecycle.count_net_new_mql) AS new_mql_sum,
      SUM(rpt_sfdc_bizible_tp_person_lifecycle.count_net_new_accepted) AS new_accepted_sum,
      0 AS first_opp_created,
      0 AS u_shaped_opp_created,
      0 AS w_shaped_opp_created,
      0 AS full_shaped_opp_created,
      0 AS custom_opp_created,
      0 AS linear_opp_created,
      0 AS first_net_arr,
      0 AS u_net_arr,
      0 AS w_net_arr,
      0 AS full_net_arr,
      0 AS custom_net_arr,
      0 AS linear_net_arr,
      0 AS first_sao,
      0 AS u_shaped_sao,
      0 AS w_shaped_sao,
      0 AS full_shaped_sao,
      0 AS custom_sao,
      0 AS linear_sao,
      0 AS pipeline_first_net_arr,
      0 AS pipeline_u_net_arr,
      0 AS pipeline_w_net_arr,
      0 AS pipeline_full_net_arr,
      0 AS pipeline_custom_net_arr,
      0 AS pipeline_linear_net_arr,
      0 AS won_first,
      0 AS won_u,
      0 AS won_w,
      0 AS won_full,
      0 AS won_custom,
      0 AS won_linear,
      0 AS won_first_net_arr,
      0 AS won_u_net_arr,
      0 AS won_w_net_arr,
      0 AS won_full_net_arr,
      0 AS won_custom_net_arr,
      0 AS won_linear_net_arr
    FROM rpt_sfdc_bizible_tp_person_lifecycle
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37
    UNION ALL
    SELECT
      rpt_sfdc_bizible_linear.bizible_touchpoint_date_month_yr AS opp_touchpoint_mo_yr, 
      rpt_sfdc_bizible_linear.bizible_touchpoint_date_normalized AS opp_touchpoint_date_normalized,
      rpt_sfdc_bizible_linear.bizible_integrated_campaign_grouping AS opp_integrated_campaign_grouping,
      rpt_sfdc_bizible_linear.marketing_channel_path,
    CASE 
      WHEN rpt_sfdc_bizible_linear.crm_user_region = 'NORAM' THEN 'AMER'
      ELSE rpt_sfdc_bizible_linear.crm_user_region 
    END AS region_normalized, --5
      IFF(rpt_sfdc_bizible_linear.crm_user_sales_segment IS null,'Unknown',rpt_sfdc_bizible_linear.crm_user_sales_segment) AS sales_segment_name,
      null AS crm_person_status,
      rpt_sfdc_bizible_linear.bizible_touchpoint_type, 
      rpt_sfdc_bizible_linear.sales_type,
      rpt_sfdc_bizible_linear.opp_created_date, --10
      rpt_sfdc_bizible_linear.sales_accepted_date,
      rpt_sfdc_bizible_linear.close_date,
      rpt_sfdc_bizible_linear.stage_name,
      rpt_sfdc_bizible_linear.is_won, 
      rpt_sfdc_bizible_linear.is_sao, --15
      rpt_sfdc_bizible_linear.deal_path_name, 
      rpt_sfdc_bizible_linear.order_type AS order_type,
      rpt_sfdc_bizible_linear.bizible_landing_page,
      rpt_sfdc_bizible_linear.bizible_form_url,
      rpt_sfdc_bizible_linear.dim_crm_account_id, --20
      rpt_sfdc_bizible_linear.dim_crm_opportunity_id AS dim_crm_opportunity_id,
      rpt_sfdc_bizible_linear.crm_account_name,
      rpt_sfdc_bizible_linear.crm_account_gtm_strategy,
      UPPER(rpt_sfdc_bizible_linear.country) AS country,
      rpt_sfdc_bizible_linear.bizible_medium AS bizible_medium, -- 25
      rpt_sfdc_bizible_linear.touchpoint_segment,
      rpt_sfdc_bizible_linear.gtm_motion,
      rpt_sfdc_bizible_linear.last_utm_campaign,
      rpt_sfdc_bizible_linear.last_utm_content,
      rpt_sfdc_bizible_linear.bizible_ad_campaign_name, --30
      rpt_sfdc_bizible_linear.lead_source,
      rpt_sfdc_bizible_linear.campaign_type,
      null AS mql_datetime_least,
      null AS true_inquiry_date,
      null AS dim_crm_person_id,
      null AS is_inquiry,
      null AS is_mql,
      0 AS total_cost,
      0 AS touchpoint_sum,
      0 AS new_lead_created_sum,
      0 AS count_true_inquiry, 
      0 AS mql_sum,
      0 AS accepted_sum,
      0 AS new_mql_sum,
      0 AS new_accepted_sum,
      SUM(rpt_sfdc_bizible_linear.first_weight) AS first_opp_created,
      SUM(rpt_sfdc_bizible_linear.u_weight) AS u_shaped_opp_created,
      SUM(rpt_sfdc_bizible_linear.w_weight) AS w_shaped_opp_created,
      SUM(rpt_sfdc_bizible_linear.full_weight) AS full_shaped_opp_created,
      SUM(rpt_sfdc_bizible_linear.custom_weight) AS custom_opp_created,
      SUM(rpt_sfdc_bizible_linear.l_weight) AS linear_opp_created,
      SUM(rpt_sfdc_bizible_linear.first_net_arr) AS first_net_arr,
      SUM(rpt_sfdc_bizible_linear.u_net_arr) AS u_net_arr,
      SUM(rpt_sfdc_bizible_linear.w_net_arr) AS w_net_arr,
      SUM(rpt_sfdc_bizible_linear.full_net_arr) AS full_net_arr,
      SUM(rpt_sfdc_bizible_linear.custom_net_arr) AS custom_net_arr,
      SUM(rpt_sfdc_bizible_linear.linear_net_arr) AS linear_net_arr,
      CASE
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.first_weight) 
        ELSE 0 
      END AS first_sao,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.u_weight) 
        ELSE 0 
      END AS u_shaped_sao,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.w_weight) 
        ELSE 0 
      END AS w_shaped_sao,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.full_weight) 
        ELSE 0 
      END AS full_shaped_sao,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.custom_weight) 
        ELSE 0 
      END AS custom_sao,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.l_weight) 
        ELSE 0 
      END AS linear_sao,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.u_net_arr) 
        ELSE 0 
      END AS pipeline_first_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.u_net_arr) 
        ELSE 0 
        END AS pipeline_u_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.w_net_arr) 
        ELSE 0 
      END AS pipeline_w_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.full_net_arr) 
        ELSE 0 
      END AS pipeline_full_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.custom_net_arr) 
        ELSE 0 
      END AS pipeline_custom_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_sao = true THEN SUM(rpt_sfdc_bizible_linear.linear_net_arr) 
        ELSE 0 
      END AS pipeline_linear_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.first_weight) 
        ELSE 0 
      END AS won_first,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.u_weight) 
        ELSE 0 
      END AS won_u,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.w_weight) 
        ELSE 0 
      END AS won_w,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.full_weight) 
        ELSE 0 
      END AS won_full,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.custom_weight) 
        ELSE 0 
      END AS won_custom,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.l_weight) 
        ELSE 0 
      END AS won_linear,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.first_net_arr) 
        ELSE 0 
      END AS won_first_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.u_net_arr) 
        ELSE 0 
      END AS won_u_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.w_net_arr) 
        ELSE 0 
      END AS won_w_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.full_net_arr) 
        ELSE 0 
      END AS won_full_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.custom_net_arr) 
        ELSE 0 
      END AS won_custom_net_arr,
      CASE 
        WHEN rpt_sfdc_bizible_linear.is_won = 'True' THEN SUM(rpt_sfdc_bizible_linear.linear_net_arr) 
        ELSE 0 
      END AS won_linear_net_arr
    FROM rpt_sfdc_bizible_linear
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37

), final AS (

    SELECT
      unioned.*,
      dim_date.fiscal_year                     AS date_range_year,
      dim_date.fiscal_quarter_name_fy          AS date_range_quarter,
      DATE_TRUNC(month, dim_date.date_actual)  AS date_range_month
    FROM unioned
    LEFT JOIN dim_date ON
    unioned.bizible_touchpoint_date_normalized=dim_date.date_actual
    WHERE bizible_touchpoint_date_normalized >= '09/01/2019'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-01-25",
    updated_date="2022-01-25"
) }}
