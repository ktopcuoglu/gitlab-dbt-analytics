{{ config({
    "materialized": "table"
    })
}}

WITH usage_data AS (

    SELECT
      *,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')     AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)             AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER         AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER         AS minor_version,
      major_version || '.' || minor_version               AS major_minor_version
    FROM {{ ref('version_usage_data_source') }}
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%') -- Messy data that's not worth parsing.
      AND hostname NOT IN ( -- Staging data has no current use cases for analysis.
        'staging.gitlab.com',
        'dr.gitlab.com'
      )

)

, raw_usage_data AS (

    SELECT *
    FROM {{ ref('version_raw_usage_data_source') }}

)

, joined AS (

    SELECT 
      usage_data.*,
      raw_usage_data.raw_usage_data_payload
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id
)

SELECT *
FROM joined
