{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('snowflake_imports_usage_ping_payloads_source') }}

), usage_data AS (

    SELECT
      {{ dbt_utils.star(from=ref('snowflake_imports_usage_ping_payloads_source'), except=['EDITION']) }},
      IFF(license_expires_at >= recorded_at OR license_expires_at IS NULL, edition, 'EE Free') AS edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                          AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                  AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                              AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                              AS minor_version,
      major_version || '.' || minor_version                                                    AS major_minor_version
    FROM source

)

SELECT *
FROM usage_data