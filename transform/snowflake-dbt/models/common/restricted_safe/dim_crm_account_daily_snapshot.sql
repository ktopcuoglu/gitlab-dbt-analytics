{{ sfdc_account_fields('TRUE') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-01-20",
    updated_date="2022-01-20"
) }}
