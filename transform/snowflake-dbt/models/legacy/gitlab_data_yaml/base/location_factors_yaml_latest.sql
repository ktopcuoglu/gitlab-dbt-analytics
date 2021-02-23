WITH source AS (

    SELECT *, 
      ROW_NUMBER() OVER (PARTITION BY area, country ORDER BY valid_from_date DESC) AS rank_location_factor_desc
    FROM {{ ref('location_factors_yaml_historical') }}

)

SELECT *
FROM source
WHERE rank_location_factor_desc = 1