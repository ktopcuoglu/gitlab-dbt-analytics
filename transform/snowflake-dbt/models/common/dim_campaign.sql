{{config({
    "schema": "legacy"
  })
}}

WITH sfdc_campaign_info AS (

    SELECT *
    FROM {{ ref('sfdc_campaign_source') }}
    WHERE NOT is_deleted

), final AS (

    SELECT
      campaign_id,
      campaign_name,
      is_active,
      status,
      type,
      description,
      budget_holder,
      bizible_touchpoint_enabled_setting,
      strategic_marketing_contribution
    FROM sfdc_campaign_info

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-11-13",
    updated_date="2020-11-13"
) }}



