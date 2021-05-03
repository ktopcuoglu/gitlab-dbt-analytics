{{ simple_cte([
    ('crm_accounts', 'dim_crm_account')
]) }}

, host_instance_type AS (

    SELECT *
    FROM {{ source('sheetload', 'host_instance_type') }}

), final AS (

    SELECT  DISTINCT
      host_instance_type.uuid                                       AS instance_uuid,
      host_instance_type.hostname                                   AS instance_hostname,
      host_instance_type.instance_type,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id')  }}     AS dim_crm_account_id,
      crm_accounts.crm_account_name
    FROM host_instance_type
    LEFT JOIN crm_accounts
      ON host_instance_type.company_sfdc_id = crm_accounts.dim_crm_account_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2021-04-01",
    updated_date="2021-05-03"
) }}
