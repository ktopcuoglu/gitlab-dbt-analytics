{{ sfdc_user_fields('base') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@michellecooper",
    created_date="2021-01-12",
    updated_date="2022-01-25"
) }}
