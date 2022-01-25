{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('sfdc_bizible_attribution_touchpoint_xf','sfdc_bizible_attribution_touchpoint_xf')
]) }}

WITH linear_base AS ( --the number of touches a given opp has in total
    --linear attribution Net_Arr of an opp / all touches (count_touches) for each opp - weighted by the number of touches in the given bucket (campaign,channel,etc)

    SELECT 
      dim_crm_opportunity_id,
      net_arr,
      COUNT(DISTINCT mart_crm_attribution_touchpoint.dim_crm_touchpoint_id) AS count_touches,
      net_arr/count_touches AS weighted_linear_net_arr
    FROM  mart_crm_attribution_touchpoint
    GROUP BY 1,2

), campaigns_per_opp AS (

    SELECT
      dim_crm_opportunity_id,
      COUNT(DISTINCT mart_crm_attribution_touchpoint.dim_campaign_id) AS campaigns_per_opp
      FROM mart_crm_attribution_touchpoint
    GROUP BY 1

), final AS (

    SELECT
      mart_crm_attribution_touchpoint.dim_crm_opportunity_id,
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
      mart_crm_attribution_touchpoint.dim_campaign_id,
      mart_crm_attribution_touchpoint.sfdc_record_id, 
      mart_crm_attribution_touchpoint.country AS country, --5
      mart_crm_attribution_touchpoint.crm_person_title,
      mart_crm_attribution_touchpoint.bizible_salesforce_campaign,
      mart_crm_attribution_touchpoint.campaign_name,
      mart_crm_attribution_touchpoint.inquiry_date,
      mart_crm_attribution_touchpoint.opportunity_close_date,
      mart_crm_attribution_touchpoint.net_arr, 
      mart_crm_attribution_touchpoint.dim_crm_account_id,
      mart_crm_attribution_touchpoint.crm_account_name,
      mart_crm_attribution_touchpoint.crm_account_gtm_strategy,
      (mart_crm_attribution_touchpoint.net_arr / campaigns_per_opp.campaigns_per_opp) AS net_arr_per_campaign, 
      linear_base.count_touches,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position,
      mart_crm_attribution_touchpoint.bizible_touchpoint_source,
      mart_crm_attribution_touchpoint.bizible_touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_ad_campaign_name, 
      mart_crm_attribution_touchpoint.bizible_ad_content,
      mart_crm_attribution_touchpoint.bizible_form_url_raw,
      mart_crm_attribution_touchpoint.bizible_landing_page_raw,
      mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
      mart_crm_attribution_touchpoint.bizible_form_url,
      mart_crm_attribution_touchpoint.bizible_landing_page,
      mart_crm_attribution_touchpoint.bizible_referrer_page,
      mart_crm_attribution_touchpoint.bizible_marketing_channel,
      CASE
        WHEN mart_crm_attribution_touchpoint.dim_parent_campaign_id = '7014M000001dn8MQAQ' THEN 'Paid Social.LinkedIn Lead Gen'
        WHEN mart_crm_attribution_touchpoint.bizible_ad_campaign_name = '20201013_ActualTechMedia_DeepMonitoringCI' THEN 'Sponsorship'
        ELSE mart_crm_attribution_touchpoint.bizible_marketing_channel_path
      END AS marketing_channel_path, 
      sfdc_bizible_attribution_touchpoint_xf.pipe_name,
      mart_crm_attribution_touchpoint.bizible_medium, 
      mart_crm_attribution_touchpoint.lead_source,
      mart_crm_attribution_touchpoint.opportunity_created_date::date AS opp_created_date,
      mart_crm_attribution_touchpoint.sales_accepted_date::date AS sales_accepted_date,
      mart_crm_attribution_touchpoint.opportunity_close_date::date AS close_date, 
      sfdc_bizible_attribution_touchpoint_xf.sales_type,
      mart_crm_attribution_touchpoint.stage_name,
      mart_crm_attribution_touchpoint.is_won,
      mart_crm_attribution_touchpoint.is_sao,
      mart_crm_attribution_touchpoint.deal_path_name,
      mart_crm_attribution_touchpoint.order_type, 
      mart_crm_attribution_touchpoint.crm_user_sales_segment,
      mart_crm_attribution_touchpoint.crm_user_region,
      DATE_TRUNC('month',mart_crm_attribution_touchpoint.bizible_touchpoint_date)::date AS bizible_touchpoint_date_month_yr,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date::date AS bizible_touchpoint_date_normalized, 
      mart_crm_attribution_touchpoint.type AS campaign_type,
      mart_crm_attribution_touchpoint.last_utm_campaign,
      mart_crm_attribution_touchpoint.last_utm_content,
      mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_attribution_touchpoint.touchpoint_segment,
      mart_crm_attribution_touchpoint.gtm_motion,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS first_weight,
      SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) AS w_weight, 
      SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) AS u_weight, 
      SUM(mart_crm_attribution_touchpoint.bizible_attribution_percent_full_path) AS full_weight,
      SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) AS custom_weight,
      COUNT(DISTINCT mart_crm_attribution_touchpoint.dim_crm_opportunity_id) AS l_touches, 
      (l_touches / count_touches) AS l_weight, 
      (mart_crm_attribution_touchpoint.net_arr * first_weight) AS first_net_arr,
      (mart_crm_attribution_touchpoint.net_arr * w_weight) AS w_net_arr,
      (mart_crm_attribution_touchpoint.net_arr * u_weight) AS u_net_arr,  
      (mart_crm_attribution_touchpoint.net_arr * full_weight) AS full_net_arr,
      (mart_crm_attribution_touchpoint.net_arr * custom_weight) AS custom_net_arr,
      (mart_crm_attribution_touchpoint.net_arr * l_weight) AS linear_net_arr
    FROM
    mart_crm_attribution_touchpoint
    LEFT JOIN linear_base ON
    mart_crm_attribution_touchpoint.dim_crm_opportunity_id = linear_base.dim_crm_opportunity_id
    LEFT JOIN  campaigns_per_opp ON 
    mart_crm_attribution_touchpoint.dim_crm_opportunity_id =      campaigns_per_opp.dim_crm_opportunity_id
    LEFT JOIN sfdc_bizible_attribution_touchpoint_xf ON
    mart_crm_attribution_touchpoint.dim_crm_touchpoint_id = sfdc_bizible_attribution_touchpoint_xf.touchpoint_id
    GROUP BY     1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-01-25",
    updated_date="2022-01-25"
) }}
