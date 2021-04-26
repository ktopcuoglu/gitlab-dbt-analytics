{{ simple_cte([
    ('crm_accounts', 'dim_crm_account','gainsight_instance_info')
]) }}

, gainsight_instance_info AS (

    SELECT *
    FROM {{ source('prep', 'gainsight_instance_info') }}

), final AS (

    SELECT  
      host_instance_type.instance_uuid                              AS instance_uuid,
      host_instance_type.hostname                                   AS instance_hostname,
      host_instance_type.instancetype                               AS instance_type,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id')  }}     AS dim_crm_account_id,
      crm_accounts.crm_account_name
    FROM gainsight_instance_info
    LEFT JOIN crm_accounts
      ON gainsight_instance_info.crm_account_id = crm_accounts.dim_crm_account_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-04-21",
    updated_date="2021-04-21"
) }}
