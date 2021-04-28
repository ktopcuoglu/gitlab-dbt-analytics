WITH prep_recurring_charge_non_amortized AS (

    SELECT
      dim_charge_id,
      dim_product_detail_id,
      dim_subscription_id,
      dim_billing_account_id,
      dim_amendment_id,
      rate_plan_name,
      rate_plan_charge_number,
      rate_plan_charge_name,
      rate_plan_charge_segment,
      rate_plan_charge_version,
      charge_type,
      rate_plan_charge_amendement_type,
      effective_start_date_id,
      effective_end_date_id,
      effective_start_date,
      effective_end_date,
      effective_start_month,
      effective_end_month,
      unit_of_measure,
      quantity,
      mrr,
      delta_mrc,
      arr,
      delta_arc,
      tcv,
      delta_tcv,
      discount_level
    FROM {{ ref('prep_recurring_charge_non_amortized') }}

)

{{ dbt_audit(
    cte_ref="prep_recurring_charge_non_amortized",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-28",
    updated_date="2021-04-28"
) }}
