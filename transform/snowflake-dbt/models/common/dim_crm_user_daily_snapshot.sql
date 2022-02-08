{{ config({
        "materialized": "incremental",
        "unique_key": "crm_user_snapshot_id",
        "tags": ["user_snapshots"],
    })
}}

{{sfdc_user_fields('snapshot') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-01-20",
    updated_date="2022-01-25"
) }}
