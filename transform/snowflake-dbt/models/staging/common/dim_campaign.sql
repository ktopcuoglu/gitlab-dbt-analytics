WITH sfdc_campaign_info AS (

    SELECT *
    FROM {{ ref('sfdc_campaign_source') }}
    WHERE account_id IS NOT NULL

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
    WHERE NOT is_deleted

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@jjstark",
    created_date="2020-06-01",
    updated_date="2020-10-15"
) }}



