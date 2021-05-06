{{ config({
    "materialized": "incremental",
    "unique_key": "dim_usage_ping_id"
    })
}}

{%- set columns = adapter.get_columns_in_relation( ref('version_usage_data_source')) -%}

WITH usage_ping AS (

    SELECT 
      id                                                                        AS dim_usage_ping_id, 
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *, 
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash, 
      OBJECT_CONSTRUCT(
        {% for column in columns %}  
          '{{ column.column | lower }}', {{ column.column | lower }}
          {% if not loop.last %}
            ,
          {% endif %}
        {% endfor %}
      )                                                                         AS raw_usage_data_payload_reconstructed
    FROM {{ ref('version_usage_data_source') }}

), raw_usage_data AS (

    SELECT *
    FROM {{ ref('version_raw_usage_data_source') }}

), map_ip_to_country AS (

    SELECT * 
    FROM {{ ref('map_ip_to_country') }}
    
), locations AS (

    SELECT * 
    FROM {{ ref('prep_location_country') }}

), usage_data AS (

    SELECT
      dim_usage_ping_id, 
      ping_created_at,
      source_ip_hash                                                                                  AS ip_address_hash,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, edition, 'EE Free')    AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                                 AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                         AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                     AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                     AS minor_version,
      major_version || '.' || minor_version                                                           AS major_minor_version,
      raw_usage_data_payload_reconstructed
    FROM usage_ping
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%') -- Messy data that's not worth parsing

), joined AS (

    SELECT 
      dim_usage_ping_id,
      ping_created_at, 
      ip_address_hash,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      original_edition, 
      IFF(original_edition = 'CE', 'CE', 'EE')                                                  AS main_edition,
      CASE 
        WHEN original_edition = 'CE'                                     THEN 'Core'
        WHEN original_edition = 'EE Free'                                THEN 'Core'                                                      
        WHEN license_expires_at < ping_created_at                        THEN 'Core'
        WHEN original_edition = 'EE'                                     THEN 'Starter'
        WHEN original_edition = 'EES'                                    THEN 'Starter'
        WHEN original_edition = 'EEP'                                    THEN 'Premium'
        WHEN original_edition = 'EEU'                                    THEN 'Ultimate'
        ELSE NULL END                                                                           AS product_tier,
      main_edition || ' - ' || product_tier                                                     AS main_edition_product_tier,
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                     AS usage_ping_delivery_type,
      CASE
        WHEN usage_ping_delivery_type = 'SaaS'                  THEN TRUE 
        WHEN installation_type = 'gitlab-development-kit'       THEN TRUE 
        WHEN hostname = 'gitlab.com'                            THEN TRUE 
        WHEN hostname ILIKE '%.gitlab.com'                      THEN TRUE 
        ELSE FALSE END                                                                          AS is_internal, 
      CASE
        WHEN hostname ilike 'staging.%'                         THEN TRUE
        WHEN hostname IN ( 
        'staging.gitlab.com',
        'dr.gitlab.com'
      )                                                         THEN TRUE
        ELSE FALSE END                                                                          AS is_staging,     
      COALESCE(raw_usage_data.raw_usage_data_payload, raw_usage_data_payload_reconstructed)     AS raw_usage_data_payload
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id

), map_ip_location AS (

    SELECT 
      map_ip_to_country.ip_address_hash, 
      map_ip_to_country.dim_location_country_id  
    FROM map_ip_to_country
    INNER JOIN locations 
      WHERE map_ip_to_country.dim_location_country_id = locations.dim_location_country_id

), add_country_info_to_usage_ping AS (

    SELECT 
      joined.*, 
      map_ip_location.dim_location_country_id 
    FROM joined 
    LEFT JOIN map_ip_location
      ON joined.ip_address_hash = map_ip_location.ip_address_hash 

), dim_product_tier AS (
  
  SELECT * 
  FROM {{ ref('dim_product_tier') }}
  WHERE product_delivery_type = 'Self-Managed'

), final AS (

    SELECT 
      dim_usage_ping_id,
      dim_product_tier.dim_product_tier_id AS dim_product_tier_id,
      ping_created_at,
      DATEADD('days', -28, ping_created_at)              AS ping_created_at_28_days_earlier,
      DATE_TRUNC('YEAR', ping_created_at)                AS ping_created_at_year,
      DATE_TRUNC('MONTH', ping_created_at)               AS ping_created_at_month,
      DATE_TRUNC('WEEK', ping_created_at)                AS ping_created_at_week,
      DATE_TRUNC('DAY', ping_created_at)                 AS ping_created_at_date,
      raw_usage_data_id                                  AS raw_usage_data_id, 
      raw_usage_data_payload, 
      license_md5,
      original_edition, 
      edition, 
      main_edition, 
      product_tier, 
      main_edition_product_tier, 
      cleaned_version,
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      ping_source,
      is_internal, 
      is_staging, 
      dim_location_country_id,
      instance_user_count
    FROM add_country_info_to_usage_ping
    LEFT OUTER JOIN dim_product_tier
    ON TRIM(LOWER(add_country_info_to_usage_ping.product_tier)) = TRIM(LOWER(dim_product_tier.product_tier_historical_short))
    AND MAIN_EDITION = 'EE'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@kathleentam",
    updated_by="@michellecooper",
    created_date="2021-01-10",
    updated_date="2021-04-30"
) }}
