WITH host_instance_type AS (

    SELECT *
    FROM {{ source('sheetload', 'host_instance_type') }}

), subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}

), final AS (

    SELECT  
            {{ get_keyed_nulls('subscriptions.dim_subscription_id') }} AS dim_subscription_id,
            host_instance_type.uuid                                    AS uuid,
            host_instance_type.hostname                                AS hostname,
            host_instance_type.company_sfdc_id                         AS dim_crm_account_id,
            host_instance_type.company_name                            AS company_name,
            host_instance_type.instance_type                           AS instance_type

    FROM host_instance_type
    LEFT OUTER JOIN subscriptions
      ON host_instance_type.subscription_id = subscriptions.dim_subscription_id
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-04-01",
    updated_date="2021-04-01"
) }}
