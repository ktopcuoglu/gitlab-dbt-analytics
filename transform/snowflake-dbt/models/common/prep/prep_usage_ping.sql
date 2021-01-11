{{ config({
    "materialized": "incremental",
    "unique_key": "dim_usage_ping_id"
    })
}}

WITH usage_ping_data AS (

    SELECT {{ hash_sensitive_columns('version_usage_data_source') }}
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid IS NOT NULL

), ip_to_geo AS (

    SELECT * 
    FROM {{ ref('dim_ip_to_geo') }}
    
), location AS (

    SELECT * 
    FROM {{ ref('location_id') }}

), location AS (

    SELECT * 
    FROM {{ ref('location_id') }}

), location AS (

    SELECT * 
    FROM {{ ref('location_id') }}

), location AS (

    SELECT * 
    FROM {{ ref('location_id') }}

), location AS (

    SELECT * 
    FROM {{ ref('location_id') }}

),















version_edition_cleaned AS (

     SELECT
        usage_ping_data.*,
        {{ get_date_id('usage_ping_data.created_at') }}              AS created_date_id,
        REGEXP_REPLACE(NULLIF(version, ''), '\-.*')                  AS cleaned_version,
        SPLIT_PART(cleaned_version, '.', 1)                          AS major_version,
        SPLIT_PART(cleaned_version, '.', 2)                          AS minor_version,
        major_version || '.' || minor_version                        AS major_minor_version,
        IFF(
            version LIKE '%-pre%' OR version LIKE '%-rc%', 
            TRUE, FALSE
        )::BOOLEAN                                                   AS is_pre_release,
        IFF(edition = 'CE', 'CE', 'EE')                              AS main_edition,
        CASE 
            WHEN edition = 'CE'                                   THEN 'Core'
            WHEN edition = 'EE Free'                              THEN 'Core'                                                      
            WHEN license_expires_at < usage_ping_data.created_at  THEN 'Core'
            WHEN edition = 'EE'                                   THEN 'Starter'
            WHEN edition = 'EES'                                  THEN 'Starter'
            WHEN edition = 'EEP'                                  THEN 'Premium'
            WHEN edition = 'EEU'                                  THEN 'Ultimate'
            ELSE NULL END                                            AS product_tier,
        main_edition || ' - ' || product_tier                        AS main_edition_product_tier, 
        IFF( uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f',
                'SaaS','Self-Managed')                               AS ping_source
    FROM usage_ping_data

), internal_identified AS (

    SELECT 
        *, 
        CASE
            WHEN ping_source = 'SaaS'                           THEN TRUE 
            WHEN installation_type = 'gitlab-development-kit'   THEN TRUE 
            WHEN hostname = 'gitlab.com'                        THEN TRUE 
            WHEN hostname ILIKE '%.gitlab.com'                  THEN TRUE 
            ELSE FALSE END                                           AS is_internal, 
        IFF(hostname ilike 'staging.%', TRUE, FALSE)                AS is_staging
    FROM version_edition_cleaned

), raw_usage_data AS (

    SELECT *
    FROM {{ ref('version_raw_usage_data_source') }}

), renamed AS (

    SELECT 
      internal_identified.*,
      raw_usage_data.raw_usage_data_payload
    FROM internal_identified
    LEFT JOIN raw_usage_data
      ON internal_identified.raw_usage_data_id = raw_usage_data.raw_usage_data_id

)

SELECT * 
FROM renamed


