{{ sfdc_opportunity_fields('base') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-02-01",
    updated_date="2022-02-01"
) }}