WITH source AS (
    SELECT *
    FROM {{ ref('geozones_yaml_flatten_source') }}
),
grouping AS (
    SELECT
      geozone_title,
      IFF(country = 'United States', geozone_title || ', ' || country, geozone_title)             AS geozone_locality,
      'All, ' || country                                                                          AS country_locality,
      geozone_factor,
      country,
      state_or_province,
      valid_from,
      valid_to,
      is_current,
      LAG(geozone_factor, 1, 0) OVER (PARTITION BY country,state_or_province ORDER BY valid_from) AS lag_factor,
      CONDITIONAL_TRUE_EVENT(geozone_factor != lag_factor)
                             OVER (PARTITION BY country,state_or_province ORDER BY valid_from)    AS locality_group
    FROM source
),
final AS (
  SELECT DISTINCT
    geozone_title,
    geozone_factor,
    geozone_locality,
    country_locality,
    country                                                                             AS geozone_country,
    state_or_province                                                                   AS geozone_state_or_province,
    MIN(valid_from) OVER (PARTITION BY country,state_or_province,locality_group)::DATE  AS valid_from,
    MAX(valid_to) OVER (PARTITION BY country,state_or_province,locality_group)::DATE    AS valid_to,
    BOOLOR_AGG(is_current) OVER (PARTITION BY country,state_or_province,locality_group) AS is_current
  FROM grouping
)

SELECT *
FROM final