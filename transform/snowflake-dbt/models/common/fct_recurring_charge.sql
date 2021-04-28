WITH prep_recurring_charge_non_amortized AS (

    SELECT *
    FROM {{ ref('prep_recurring_charge_non_amortized') }}

)

{{ dbt_audit(
    cte_ref="prep_recurring_charge_non_amortized",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-28",
    updated_date="2021-04-28"
) }}
