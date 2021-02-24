WITH source AS (

    SELECT *
    FROM {{ ref('location_factors_yaml_source') }}
  
), filtered as (

    SELECT
      IFF(area = 'Zug/Zurig','Zug/Zurich', area)                    AS area, 
      country,
      location_factor,
      IFF(snapshot_date = '2021-01-03','2021-01-01', snapshot_date) AS valid_from_date,
      MAX(snapshot_date) OVER (PARTITION BY area, country)          AS last_snapshot_date
      --we didn't start capturing data until 21.01.03 but adjusting for downstream models 
    FROM source
    QUALIFY ROW_NUMBER() OVER (PARTITION BY area, country, location_factor ORDER BY snapshot_date) = 1

), final AS (

    SELECT
      area,
      country,
      location_factor,
      IFF(area IS NULL, country, TRIM(area ||', '||country))        AS yaml_locality,
      valid_from_date,
      COALESCE(LEAD(DATEADD(day,-1,valid_from_date)) 
                    OVER (PARTITION BY area, country ORDER BY valid_from_date),
                last_snapshot_date)                                 AS valid_to_date
    FROM filtered

)

SELECT *
FROM final