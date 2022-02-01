{{ config({
        "materialized": "incremental",
        "unique_key": "crm_opportunity_snapshot_id",
        "tags": ["opportunity_snapshots"],
    })
}}

{{ sfdc_opportunity_fields('snapshot') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-02-01",
    updated_date="2022-02-01"
) }}
