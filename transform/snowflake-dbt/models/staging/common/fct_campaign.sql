{{config({
    "schema": "common"
  })
}}

WITH sfdc_campaigns AS (

    SELECT *
    FROM {{ ref('sfdc_campaign_source') }}
    WHERE NOT is_deleted

), final_campaigns AS (

    SELECT

      -- campaign ids
      campaign_id                                   AS dim_campaign_id,
      campaign_parent_id,

      -- user ids
      campaign_owner_id,
      created_by_id,
      last_modified_by_id,

      -- dates
      start_date,
      {{ get_date_id('start_date') }}              AS start_date_id,
      end_date,
      {{ get_date_id('end_date') }}                AS end_date_id,
      created_date,
      {{ get_date_id('created_date') }}            AS created_date_id,
      last_modified_date,
      {{ get_date_id('last_modified_date') }}      AS last_modified_date_id,
      last_activity_date,
      {{ get_date_id('last_activity_date') }}      AS last_activity_date_id,

      region,
      sub_region,

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

    FROM sfdc_campaigns

)

{{ dbt_audit(
    cte_ref="final_campaigns",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-11-19",
    updated_date="2020-11-19"
) }}
