{{ config({
    "materialized": "incremental",
    "unique_key": "dim_usage_ping_id"
    })
}}

{%- set columns = adapter.get_columns_in_relation( ref('version_usage_data_source')) -%}

WITH source AS (

    SELECT 
      id                                                                        AS dim_usage_ping_id, 
      created_at                                                                AS ping_created_at,
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

), map_ip_to_geo AS (

    SELECT * 
    FROM {{ ref('map_ip_to_geo') }}
    
), location AS (

    SELECT * 
    FROM {{ ref('dim_location') }}

), usage_data AS (

    SELECT
      dim_usage_ping_id, 
      ping_created_at,
      ip_address_hash,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      edition                                                                                         AS original_edition,
      IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, edition, 'EE Free')    AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                                 AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                         AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                     AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                     AS minor_version,
      major_version || '.' || minor_version                                                           AS major_minor_version,
      raw_usage_data_payload_reconstructed
    FROM source
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%') -- Messy data that's not worth parsing

), joined AS (

    SELECT 
      dim_usage_ping_id,
      ping_created_at, 
      ip_address_hash,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP']) }},
      original_edition, 
      cleaned_edition                                                                           AS edition,
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
      cleaned_version,
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f'      THEN 'SaaS'
        ELSE 'Self-Managed'
        END                                                                                     AS ping_source,
      CASE
        WHEN ping_source = 'SaaS'                               THEN TRUE 
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
      map_ip_to_geo.ip_address_hash, 
      map_ip_to_geo.dim_location_id, 
      location.country_name, 
      location.iso_2_country_code, 
      location.iso_3_country_code  
    FROM map_ip_to_geo
    INNER JOIN location 
      WHERE map_ip_to_geo.dim_location_id = location.dim_location_id

), add_country_info_to_usage_ping AS (

    SELECT 
      joined.*, 
      map_ip_location.dim_location_id, 
      map_ip_location.country_name, 
      map_ip_location.iso_2_country_code, 
      map_ip_location.iso_3_country_code  
    FROM joined 
    LEFT JOIN map_ip_location
      ON joined.ip_address_hash = map_ip_location.ip_address_hash 

), final AS (

    SELECT 
      dim_usage_ping_id,
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
      country_name, 
      iso_2_country_code, 
      iso_3_country_code  
    FROM add_country_info_to_usage_ping

)

SELECT * 
FROM final
