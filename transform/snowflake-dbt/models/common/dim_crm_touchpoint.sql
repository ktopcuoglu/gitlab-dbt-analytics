WITH campaign_details AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), bizible_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), bizible_attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), bizible_touchpoints_with_campaign AS (

    SELECT
      bizible_touchpoints.*,
      campaign_details.dim_campaign_id,
      campaign_details.dim_parent_campaign_id
    FROM bizible_touchpoints
    LEFT JOIN campaign_details
      ON bizible_touchpoints.campaign_id = campaign_details.dim_campaign_id

), bizible_attribution_touchpoints_with_campaign AS (

    SELECT
      bizible_attribution_touchpoints.*,
      campaign_details.dim_campaign_id,
      campaign_details.dim_parent_campaign_id
    FROM bizible_attribution_touchpoints
    LEFT JOIN campaign_details
      ON bizible_attribution_touchpoints.campaign_id = campaign_details.dim_campaign_id

), bizible_campaign_grouping AS (

    SELECT *
    FROM {{ ref('map_bizible_campaign_grouping') }}

), combined_touchpoints AS (

    SELECT
      --ids
      touchpoint_id                 AS dim_crm_touchpoint_id,
      -- touchpoint info
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_source_type,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_salesforce_campaign,
      '0'                           AS is_attribution_touchpoint,
      dim_campaign_id,
      dim_parent_campaign_id

    FROM bizible_touchpoints_with_campaign

    UNION ALL

    SELECT
      --ids
      touchpoint_id                 AS dim_crm_touchpoint_id,
      -- touchpoint info
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_source_type,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_salesforce_campaign,
      '1'                           AS is_attribution_touchpoint,
      dim_campaign_id,
      dim_parent_campaign_id

    FROM bizible_attribution_touchpoints_with_campaign

), final AS (

    SELECT
      combined_touchpoints.dim_crm_touchpoint_id,
      combined_touchpoints.bizible_touchpoint_date,
      combined_touchpoints.bizible_touchpoint_position,
      combined_touchpoints.bizible_touchpoint_source,
      combined_touchpoints.bizible_touchpoint_source_type,
      combined_touchpoints.bizible_touchpoint_type,
      combined_touchpoints.bizible_ad_campaign_name,
      combined_touchpoints.bizible_ad_content,
      combined_touchpoints.bizible_ad_group_name,
      combined_touchpoints.bizible_form_url,
      combined_touchpoints.bizible_form_url_raw,
      combined_touchpoints.bizible_landing_page,
      combined_touchpoints.bizible_landing_page_raw,
      combined_touchpoints.bizible_marketing_channel,
      combined_touchpoints.bizible_marketing_channel_path,
      combined_touchpoints.bizible_medium,
      combined_touchpoints.bizible_referrer_page,
      combined_touchpoints.bizible_referrer_page_raw,
      combined_touchpoints.bizible_salesforce_campaign,
      combined_touchpoints.is_attribution_touchpoint,
      bizible_campaign_grouping.integrated_campaign_grouping,
      bizible_campaign_grouping.bizible_integrated_campaign_grouping,
      bizible_campaign_grouping.gtm_motion,
      bizible_campaign_grouping.touchpoint_segment,
      CASE
        WHEN combined_touchpoints.dim_crm_touchpoint_id ILIKE 'a6061000000CeS0%' -- Specific touchpoint overrides
          THEN 'Field Event'
        WHEN combined_touchpoints.bizible_marketing_channel_path = 'CPC.AdWords'
          THEN 'Google AdWords'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
          THEN 'Email'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference','Speaking Session')
                  OR (combined_touchpoints.bizible_medium = 'Field Event (old)' AND combined_touchpoints.bizible_marketing_channel_path = 'Other')
          THEN 'Field Event'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Paid Social.Facebook','Paid Social.LinkedIn','Paid Social.Twitter','Paid Social.YouTube')
          THEN 'Paid Social'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Social.Facebook','Social.LinkedIn','Social.Twitter','Social.YouTube')
          THEN 'Social'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Marketing Site.Web Referral','Web Referral')
          THEN 'Web Referral'
        WHEN combined_touchpoints.bizible_marketing_channel_path in ('Marketing Site.Web Direct', 'Web Direct')
              -- Added to Web Direct
              OR combined_touchpoints.dim_campaign_id in (
                                '701610000008ciRAAQ', -- Trial - GitLab.com
                                '70161000000VwZbAAK', -- Trial - Self-Managed
                                '70161000000VwZgAAK', -- Trial - SaaS
                                '70161000000CnSLAA0', -- 20181218_DevOpsVirtual
                                '701610000008cDYAAY'  -- 2018_MovingToGitLab
                                )
          THEN 'Web Direct'
        WHEN combined_touchpoints.bizible_marketing_channel_path LIKE 'Organic Search.%'
              OR combined_touchpoints.bizible_marketing_channel_path = 'Marketing Site.Organic'
          THEN 'Organic Search'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Sponsorship')
          THEN 'Paid Sponsorship'
        ELSE 'Unknown'
      END AS pipe_name
    FROM combined_touchpoints
    LEFT JOIN bizible_campaign_grouping
      ON combined_touchpoints.dim_crm_touchpoint_id = bizible_campaign_grouping.dim_crm_touchpoint_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2021-01-21",
    updated_date="2021-11-09"
) }}
