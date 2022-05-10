{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "dim_ping_instance_id"
) }}


{{ simple_cte([
    ('raw_usage_data', 'version_raw_usage_data_source')
    ])

}}

, source AS (

    SELECT
      id                                                                        AS dim_ping_instance_id,
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *,
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash
    FROM {{ ref('version_usage_data_source') }} as usage

  {% if is_incremental() %}
          WHERE ping_created_at >= (SELECT MAX(ping_created_at) FROM {{this}})
  {% endif %}

), usage_data AS (

    SELECT
      dim_ping_instance_id                                                                                                    AS dim_ping_instance_id,
      host_id                                                                                                                 AS dim_host_id,
      uuid                                                                                                                    AS dim_instance_id,
      ping_created_at                                                                                                         AS ping_created_at,
      source_ip_hash                                                                                                          AS ip_address_hash,
      edition                                                                                                                 AS original_edition,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }}
    FROM source
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%')

), joined_ping AS (

    SELECT
      dim_ping_instance_id                                                                                                                        AS dim_ping_instance_id,
      dim_host_id                                                                                                                                 AS dim_host_id,
      usage_data.dim_instance_id                                                                                                                  AS dim_instance_id,
      {{ dbt_utils.surrogate_key(['dim_host_id', 'dim_instance_id'])}}                                                                            AS dim_installation_id,
      ping_created_at                                                                                                                             AS ping_created_at,
      ip_address_hash                                                                                                                             AS ip_address_hash,
      original_edition                                                                                                                            AS original_edition,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      IFF(original_edition = 'CE', 'CE', 'EE')                                                                                                    AS main_edition,
      CASE
        WHEN original_edition = 'CE'                                     THEN 'Core'
        WHEN original_edition = 'EE Free'                                THEN 'Core'
        WHEN license_expires_at < ping_created_at                        THEN 'Core'
        WHEN original_edition = 'EE'                                     THEN 'Starter'
        WHEN original_edition = 'EES'                                    THEN 'Starter'
        WHEN original_edition = 'EEP'                                    THEN 'Premium'
        WHEN original_edition = 'EEU'                                    THEN 'Ultimate'
        ELSE NULL END                                                                                                                             AS product_tier,
        COALESCE(raw_usage_data.raw_usage_data_payload, usage_data.raw_usage_data_payload_reconstructed)                                          AS raw_usage_data_payload
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id

)

{{ dbt_audit(
    cte_ref="joined_ping",
    created_by="@icooper-acp",
    updated_by="@snalamaru",
    created_date="2022-03-17",
    updated_date="2022-05-05"
) }}