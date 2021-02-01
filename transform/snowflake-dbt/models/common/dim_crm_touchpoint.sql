WITH bizible_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), bizible_attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

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
      '0'                           AS is_attribution_touchpoint

    FROM bizible_touchpoints

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
      '1'                           AS is_attribution_touchpoint

    FROM bizible_attribution_touchpoints

)

{{ dbt_audit(
    cte_ref="combined_touchpoints",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-01-21",
    updated_date="2021-01-21"
) }}
