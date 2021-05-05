{{ simple_cte([
    ('crm_accounts', 'dim_crm_account'),
    ('gainsight_instance_info', 'gainsight_instance_info_source')
]) }}

, final AS (

    SELECT DISTINCT
      gainsight_instance_info.instance_uuid                         AS instance_uuid,
      gainsight_instance_info.instance_hostname                     AS instance_hostname,
      gainsight_instance_info.instance_type                         AS instance_type,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id')  }}     AS dim_crm_account_id,
      crm_accounts.crm_account_name
    FROM gainsight_instance_info
    LEFT JOIN crm_accounts
      ON gainsight_instance_info.crm_account_id = crm_accounts.dim_crm_account_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2021-04-01",
    updated_date="2021-05-03"
) }}
