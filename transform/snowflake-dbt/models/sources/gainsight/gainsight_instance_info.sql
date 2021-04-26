WITH source AS (

    SELECT *
    FROM {{ source('gainsight', 'gainsight_instance_info') }}

), final AS (

    SELECT 
      crm_acct_id                                          AS crm_account_id,
      gainsight_unique_row_id                              AS gainsight_unique_row_id,
      instance_uuid                                        AS instance_uuid,
      hostname                                             AS instance_hostname,
      instancetype                                         AS instance_type,
      to_timestamp(_updated_at::INT)                       AS uploaded_at
    FROM source

)

SELECT * 
FROM final
