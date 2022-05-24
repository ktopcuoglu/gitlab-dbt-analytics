{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('mart_crm_touchpoint','mart_crm_touchpoint'),
    ('rpt_crm_person_with_opp','rpt_crm_person_with_opp')
]) }}

, base AS (

  SELECT DISTINCT
    mart_crm_touchpoint.dim_crm_person_id,
    mart_crm_touchpoint.email_hash,
    mart_crm_touchpoint.dim_crm_touchpoint_id,
    mart_crm_touchpoint.mql_date_first,
    mart_crm_touchpoint.bizible_touchpoint_date
    FROM mart_crm_touchpoint
    WHERE mql_date_first IS NOT null

), count_of_pre_mql_tps AS (

  SELECT DISTINCT
    email_hash,
    COUNT(DISTINCT dim_crm_touchpoint_id) AS pre_mql_touches
  FROM base
  WHERE bizible_touchpoint_date <= mql_date_first
  GROUP BY 1

), pre_mql_tps_by_person AS (
    
    SELECT
      email_hash,
      pre_mql_touches,
      1/pre_mql_touches AS pre_mql_weight
    FROM count_of_pre_mql_tps
    GROUP BY 1,2
    
), pre_mql_tps AS (
      
    SELECT
      base.dim_crm_touchpoint_id,
      pre_mql_tps_by_person.pre_mql_weight
    FROM pre_mql_tps_by_person
    LEFT JOIN base ON
    pre_mql_tps_by_person.email_hash=base.email_hash
    WHERE bizible_touchpoint_date <= mql_date_first
      
), post_mql_tps AS (
    
    SELECT
      base.dim_crm_touchpoint_id,
      0 AS pre_mql_weight
    FROM base
    WHERE bizible_touchpoint_date > mql_date_first
      OR mql_date_first IS null

), mql_weighted_tps AS (

    SELECT *
    FROM pre_mql_tps
    UNION ALL
    SELECT *
    FROM post_mql_tps

), final AS (

    SELECT DISTINCT
      DATE_TRUNC('month',mart_crm_touchpoint.bizible_touchpoint_date)::date AS bizible_touchpoint_date_month_yr,
      mart_crm_touchpoint.bizible_touchpoint_date::date AS bizible_touchpoint_date_normalized,
      mart_crm_touchpoint.bizible_touchpoint_date,
      mart_crm_touchpoint.dim_crm_touchpoint_id,
      mart_crm_touchpoint.bizible_touchpoint_type,
      mart_crm_touchpoint.bizible_touchpoint_source,
      mart_crm_touchpoint.bizible_medium,
      mart_crm_touchpoint.dim_crm_person_id,
      mart_crm_touchpoint.email_hash,
      mart_crm_touchpoint.sfdc_record_id,
      mart_crm_touchpoint.lead_source,
      mart_crm_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_touchpoint.campaign_name,
      mart_crm_touchpoint.type AS campaign_type,
      CASE
        WHEN mart_crm_touchpoint.dim_campaign_id = '7014M000001dn8MQAQ' THEN 'Paid Social.LinkedIn Lead Gen'
        WHEN mart_crm_touchpoint.bizible_ad_campaign_name = '20201013_ActualTechMedia_DeepMonitoringCI' THEN 'Sponsorship'
        WHEN bizible_marketing_channel_path = 'Other' AND dim_parent_campaign_id LIKE '%7014M000001dn8M%' THEN 'Paid Social.LinkedIn Lead Gen'
        WHEN bizible_marketing_channel_path = 'Content.Gated Content' AND dim_parent_campaign_id LIKE '%7014M000001dn8M%' THEN 'Paid Social.LinkedIn Lead Gen'
        WHEN bizible_marketing_channel_path IS null AND dim_parent_campaign_id LIKE '%7014M000001dn8M%' THEN 'Paid Social.LinkedIn Lead Gen'
        ELSE mart_crm_touchpoint.bizible_marketing_channel_path
      END AS bizible_marketing_channel_path,
      mart_crm_touchpoint.bizible_landing_page,
      mart_crm_touchpoint.bizible_form_url,
      mart_crm_touchpoint.bizible_referrer_page,
      mart_crm_touchpoint.bizible_ad_campaign_name,
      mart_crm_touchpoint.bizible_ad_content,
      mart_crm_touchpoint.bizible_form_url_raw,
      mart_crm_touchpoint.bizible_landing_page_raw, 
      mart_crm_touchpoint.bizible_referrer_page_raw,
      mart_crm_touchpoint.inquiry_date,
      rpt_crm_person_with_opp.true_inquiry_date,
      mart_crm_touchpoint.mql_date_first,
      mart_crm_touchpoint.mql_date_latest,
      rpt_crm_person_with_opp.mql_inferred_date,
      LEAST(IFNULL(mart_crm_touchpoint.mql_date_first:: date,'9999-01-01'),IFNULL(rpt_crm_person_with_opp.mql_inferred_date:: date,'9999-01-01')) AS mql_datetime_least,
      mart_crm_touchpoint.accepted_date,
      mart_crm_touchpoint.crm_person_status,
      rpt_crm_person_with_opp.account_demographics_geo AS region,
      rpt_crm_person_with_opp.sales_segment_name,
      rpt_crm_person_with_opp.is_inquiry,
      rpt_crm_person_with_opp.is_mql,
      mql_weighted_tps.pre_mql_weight,
      mart_crm_touchpoint.crm_account_name,
      mart_crm_touchpoint.dim_crm_account_id,
      mart_crm_touchpoint.crm_account_gtm_strategy,
      mart_crm_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_touchpoint.touchpoint_segment,
      mart_crm_touchpoint.is_fmm_influenced,
      mart_crm_touchpoint.gtm_motion,
      mart_crm_touchpoint.crm_person_title,
      mart_crm_touchpoint.bizible_touchpoint_position,
      UPPER(mart_crm_touchpoint.crm_person_country) as person_country,
      mart_crm_touchpoint.last_utm_campaign,
      mart_crm_touchpoint.last_utm_content,
      1 AS touchpoint_count,
      CASE
        WHEN bizible_touchpoint_position LIKE '%LC%' AND bizible_touchpoint_position NOT LIKE '%PostLC%' THEN '1'
        ELSE '0'
      END AS count_inquiry,
      CASE
        WHEN true_inquiry_date >= bizible_touchpoint_date_normalized THEN '1'
        ELSE '0'
      END AS count_true_inquiry,
      CASE
        WHEN mart_crm_touchpoint.mql_date_first >= bizible_touchpoint_date_normalized THEN '1'
        ELSE '0'
      END AS count_mql,
      CASE
        WHEN rpt_crm_person_with_opp.mql_inferred_date >= bizible_touchpoint_date_normalized THEN '1'
        ELSE '0'
      END AS count_mql_inferred,
      CASE
        WHEN mql_datetime_least >= bizible_touchpoint_date_normalized THEN '1'
        ELSE '0'
      END AS count_mql_least,
      CASE 
        WHEN count_mql=1 THEN mart_crm_touchpoint.sfdc_record_id
        ELSE NULL
      END AS mql_person,
      CASE 
        WHEN count_mql_inferred=1 THEN mart_crm_touchpoint.sfdc_record_id
        ELSE NULL
      END AS mql_person_inferred,
      CASE 
        WHEN count_mql_least=1 THEN mart_crm_touchpoint.sfdc_record_id
      ELSE NULL
      END AS mql_person_least,
      CASE
        WHEN mart_crm_touchpoint.mql_date_first >= bizible_touchpoint_date_normalized THEN mart_crm_touchpoint.bizible_count_lead_creation_touch
        ELSE '0'
      END AS count_net_new_mql,
      CASE
        WHEN rpt_crm_person_with_opp.mql_inferred_date >= bizible_touchpoint_date_normalized THEN mart_crm_touchpoint.bizible_count_lead_creation_touch
        ELSE '0'
      END AS count_net_new_mql_inferred,
      CASE
        WHEN mql_datetime_least >= bizible_touchpoint_date_normalized THEN mart_crm_touchpoint.bizible_count_lead_creation_touch
        ELSE '0'
      END AS count_net_new_mql_least,
      CASE
        WHEN mart_crm_touchpoint.accepted_date >= bizible_touchpoint_date_normalized THEN '1'
        ELSE '0'
      END AS count_accepted,
      CASE
        WHEN mart_crm_touchpoint.accepted_date >= bizible_touchpoint_date_normalized THEN mart_crm_touchpoint.bizible_count_lead_creation_touch
        ELSE '0'
      END AS count_net_new_accepted
      FROM
      mart_crm_touchpoint
      LEFT JOIN mql_weighted_tps ON
      mart_crm_touchpoint.dim_crm_touchpoint_id=mql_weighted_tps.dim_crm_touchpoint_id
      LEFT JOIN rpt_crm_person_with_opp ON
      mart_crm_touchpoint.dim_crm_person_id=rpt_crm_person_with_opp.dim_crm_person_id
      WHERE bizible_touchpoint_date_normalized >= '09/01/2019'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-01-25",
    updated_date="2022-05-23"
) }}
