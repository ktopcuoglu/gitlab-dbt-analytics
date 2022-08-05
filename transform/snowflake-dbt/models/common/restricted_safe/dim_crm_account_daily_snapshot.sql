-- depends_on: {{ ref('driveload_lam_corrections_source') }}
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
    updated_by="@paul_armstrong",
    created_date="2022-01-25",
    updated_date="2022-07-22"
) }}
