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
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,
      bizible_marketing_channel,
      marketing_channel_path        AS bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
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
      combined_touchpoints.is_attribution_touchpoint,
      bizible_campaign_grouping.integrated_campaign_grouping,
      bizible_campaign_grouping.bizible_integrated_campaign_grouping,
      bizible_campaign_grouping.gtm_motion ,
      bizible_campaign_grouping.touchpoint_segment                         

    FROM combined_touchpoints
    LEFT JOIN bizible_campaign_grouping
      ON combined_touchpoints.dim_campaign_id = bizible_campaign_grouping.dim_campaign_id
        AND combined_touchpoints.dim_parent_campaign_id = bizible_campaign_grouping.dim_parent_campaign_id
        AND combined_touchpoints.bizible_touchpoint_type = bizible_campaign_grouping.bizible_touchpoint_type
        AND combined_touchpoints.bizible_landing_page = bizible_campaign_grouping.bizible_landing_page
        AND combined_touchpoints.bizible_referrer_page = bizible_campaign_grouping.bizible_referrer_page
        AND combined_touchpoints.bizible_form_url = bizible_campaign_grouping.bizible_form_url
        AND combined_touchpoints.bizible_ad_campaign_name = bizible_campaign_grouping.bizible_ad_campaign_name
        AND combined_touchpoints.bizible_marketing_channel_path = bizible_campaign_grouping.bizible_marketing_channel_path
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-21",
    updated_date="2021-03-02"
) }}
