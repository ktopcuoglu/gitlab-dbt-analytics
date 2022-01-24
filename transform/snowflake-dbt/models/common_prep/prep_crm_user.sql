{{ sfdc_user_fields('FALSE') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@iweeks",
    created_date="2021-01-12",
    updated_date="2021-04-22"
) }}
