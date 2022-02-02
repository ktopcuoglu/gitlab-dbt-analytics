{{ sfdc_account_fields('base') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@michellecooper",
    created_date="2020-06-01",
    updated_date="2022-01-25"
) }}
