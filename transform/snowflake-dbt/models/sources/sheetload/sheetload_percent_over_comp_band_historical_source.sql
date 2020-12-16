WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'percent_over_comp_band_historical') }}

), intermediate AS (

    SELECT
      NULLIF("breakout_type",'')::VARCHAR           AS breakout_type,
      "date_actual"::DATE                           AS date_actual,
      NULLIF("division",'')::VARCHAR                AS division,
      NULLIF("department",'')::VARCHAR              AS department,
      NULLIF("sum_of_weighted_out_of_band",'')FLOAT AS sum_of_weighted_out_of_band,
      NULLIF("total_employees",'')FLOAT             AS total_employees, 
      NULLIF("percent_over_comp_band",'')FLOAT      AS percent_over_comp_band
    FROM source

)

SELECT *
FROM source
