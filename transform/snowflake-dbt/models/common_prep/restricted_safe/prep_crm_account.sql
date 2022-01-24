{{ sfdc_account_fields('FALSE') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@mcooper",
    created_date="2020-06-01",
    updated_date="2022-01-20"
) }}
