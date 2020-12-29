{{ config({
    "materialized": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'sheetload_comp_band_snapshots') }}

), renamed AS (

    SELECT
      employee_number,
      percent_over_top_end_of_band,
      CASE 
        WHEN NULLIF(LOWER(percent_over_top_end_of_band), '') ='exec'    
          THEN 0.00
        WHEN NULLIF(percent_over_top_end_of_band, '') ='#DIV/0!' 
          THEN NULL
        WHEN percent_over_top_end_of_band LIKE '%'               
          THEN NULLIF(REPLACE(percent_over_top_end_of_band,'%',''),'') 
        ELSE NULLIF(percent_over_top_end_of_band, '') END                       AS percent_over_top_end_of_band_cleaned,
      dbt_valid_from::date                                                      AS valid_from,
      dbt_valid_to::DATE                               AS valid_to
    FROM source
    WHERE percent_over_top_end_of_band IS NOT NULL

), deduplicated AS (

    SELECT DISTINCT   
      employee_number,
      percent_over_top_end_of_band                                                  AS original_value,
      IFF(CONTAINS(percent_over_top_end_of_band,'%') = True,
          ROUND(percent_over_top_end_of_band_cleaned/100::FLOAT, 4),
          ROUND(percent_over_top_end_of_band_cleaned::FLOAT, 4))                    AS deviation_from_comp_calc,
    valid_from,
    valid_to
    FROM renamed

), final AS (

  SELECT
    employee_number,
    original_value,
    deviation_from_comp_calc,
    MIN(valid_from)                     AS valid_from,
    NULLIF(MAX(valid_to), CURRENT_DATE) AS valid_to
  FROM deduplicated
  GROUP BY 1, 2, 3

)

SELECT *
FROM final
