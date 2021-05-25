WITH sfdc_campaign_info AS (

    SELECT *
    FROM {{ ref('sfdc_campaign_source') }}
    WHERE NOT is_deleted

), final AS (

    SELECT
      -- campaign ids
      campaign_id                                   AS dim_campaign_id,
      campaign_parent_id                            AS dim_parent_campaign_id,

      -- campaign details
      campaign_name,
      is_active,
      status,
      type,
      description,
      budget_holder,
      bizible_touchpoint_enabled_setting,
      strategic_marketing_contribution,
      region,
      sub_region,
      large_bucket,
      reporting_type,
      allocadia_id,
      is_a_channel_partner_involved,
      is_an_alliance_partner_involved,
      is_this_an_in_person_event,
      alliance_partner_name,
      channel_partner_name,
      sales_play,
      gtm_motion,

      -- user ids
      campaign_owner_id,
      created_by_id,
      last_modified_by_id,

      -- dates
      start_date,
      end_date,
      created_date,
      last_modified_date,
      last_activity_date,

      -- additive fields
      budgeted_cost,
      expected_response,
      expected_revenue,
      actual_cost,
      amount_all_opportunities,
      amount_won_opportunities,
      count_contacts,
      count_converted_leads,
      count_leads,
      count_opportunities,
      count_responses,
      count_won_opportunities,
      count_sent

    FROM sfdc_campaign_info

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2021-03-01",
    updated_date="2021-03-01"
) }}
