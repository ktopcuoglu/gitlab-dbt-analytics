WITH sfdc_campaign_info AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), final AS (

    SELECT
      dim_campaign_id,
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
    updated_by="@mcooperDD",
    created_date="2020-11-13",
    updated_date="2021-03-01"
) }}
