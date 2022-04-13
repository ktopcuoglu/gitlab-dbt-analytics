WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_hp_dvr_source') }}

)

SELECT *
    date::TIMESTAMP             AS date,
    region::VARCHAR             AS region,
    country::VARCHAR            AS country,
    name::VARCHAR               AS name,
    numberrange::NUMBER         AS numberrange,
    alphanumeric::VARCHAR       AS alphanumeric,
    _updated_at::TIMESTAMP      AS _updated_at
FROM source
