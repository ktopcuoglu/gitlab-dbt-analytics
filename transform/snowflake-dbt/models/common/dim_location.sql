WITH maxmind_countries_source AS (

    SELECT *
    FROM {{ ref('sheetload_maxmind_countries_source') }}

), zuora_country_geographic_region AS (

    SELECT *
    FROM {{ ref('zuora_country_geographic_region') }}

), joined AS (

    SELECT
      geoname_id              AS dim_location_id,
      country_name            AS country_name,
      UPPER(country_iso_code) AS iso_2_country_code,
      UPPER(iso_alpha_3_code) AS iso_3_country_code
    FROM maxmind_countries_source
    LEFT JOIN  zuora_country_geographic_region
      ON UPPER(maxmind_countries_source.country_iso_code) = UPPER(zuora_country_geographic_region.iso_alpha_2_code)
    WHERE country_iso_code IS NOT NULL
) 


{{ dbt_audit(
    cte_ref="joined",
    created_by="@m_walker",
    updated_by="@kathleentam",
    created_date="2020-08-25",
    updated_date="2021-01-10"
) }}
