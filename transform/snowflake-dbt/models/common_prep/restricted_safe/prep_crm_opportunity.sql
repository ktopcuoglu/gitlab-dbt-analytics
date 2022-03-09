{{ sfdc_opportunity_fields('live') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-02-01",
    updated_date="2022-02-28"
) }}