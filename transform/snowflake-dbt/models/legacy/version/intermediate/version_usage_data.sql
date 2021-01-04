{{ config({
    "materialized": "table"
    })
}}

{%- set columns = adapter.get_columns_in_relation( ref('version_usage_data_source')) -%}

WITH source AS (

    SELECT 
      *, 
      OBJECT_CONSTRUCT(
        {% for column in columns %}  
          '{{ column.column | lower }}', {{ column.column | lower }}
          {% if not loop.last %}
            ,
          {% endif %}
        {% endfor %}
      ) AS raw_usage_data_payload_reconstructed
    FROM {{ ref('version_usage_data_source') }}

), usage_data AS (

    SELECT
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION']) }},
      IFF(license_expires_at >= created_at OR license_expires_at IS NULL, edition, 'EE Free') AS cleaned_edition,
      REGEXP_REPLACE(NULLIF(version, ''), '[^0-9.]+')                                         AS cleaned_version,
      IFF(version ILIKE '%-pre', True, False)                                                 AS version_is_prerelease,
      SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                             AS major_version,
      SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                             AS minor_version,
      major_version || '.' || minor_version                                                   AS major_minor_version,
      raw_usage_data_payload_reconstructed
    FROM source
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
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION']) }},
      cleaned_edition                                                                       AS edition,
      cleaned_version,
      version_is_prerelease,
      major_version,
      minor_version,
      major_minor_version,
      COALESCE(raw_usage_data.raw_usage_data_payload, raw_usage_data_payload_reconstructed) AS raw_usage_data_payload
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id
)

SELECT *
FROM joined
