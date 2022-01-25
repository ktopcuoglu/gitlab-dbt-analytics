{{ config({
        "materialized": "incremental",
        "unique_key": "crm_account_snapshot_id",
        "tags": ["account_snapshots"],
    })
}}

{{ sfdc_account_fields('snapshot') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-01-25",
    updated_date="2022-01-25"
) }}
