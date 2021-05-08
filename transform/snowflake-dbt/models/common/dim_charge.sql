WITH prep_charge AS (

    SELECT *
    FROM {{ ref('prep_charge') }}

), final AS (

    SELECT
      --Surrogate Key
      dim_charge_id,

      --Natural Key
      subscription_name,
      subscription_version,
      rate_plan_charge_number,
      rate_plan_charge_version,
      rate_plan_charge_segment,

      --Information
      rate_plan_name,
      rate_plan_charge_name,
      discount_level,
      charge_type,
      rate_plan_charge_amendement_type,
      unit_of_measure,

      --Dates
      effective_start_date,
      effective_end_date,
      effective_start_month,
      effective_end_month
    FROM prep_charge

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-05-10",
    updated_date="2021-05-10"
) }}
