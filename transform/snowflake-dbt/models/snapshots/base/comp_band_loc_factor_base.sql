{{ config({
    "materialized": "ephemeral"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'sheetload_employee_location_factor_snapshots') }}
    WHERE "Employee_ID" != 'Not In Comp Calc'
      AND "Employee_ID" NOT IN ('$72,124','S1453')

), renamed AS (

    SELECT
      NULLIF("Employee_ID",'')::VARCHAR                                     AS employee_number,
      deviation_from_comp_calc                                              AS original_value,
      CASE 
        WHEN NULLIF(deviation_from_comp_calc, '') ='Exec'    
          THEN '0.00'
        WHEN NULLIF(deviation_from_comp_calc, '') ='#DIV/0!' 
          THEN NULL
        WHEN deviation_from_comp_calc LIKE '%'               
          THEN NULLIF(REPLACE(deviation_from_comp_calc,'%',''),'') 
        ELSE NULLIF(deviation_from_comp_calc, '') END                       AS deviation_from_comp_calc_cl,
      IFF("DBT_VALID_FROM"::NUMBER::TIMESTAMP::DATE < '2019-10-18'::date,
           '2000-01-20'::DATE,
           "DBT_VALID_FROM"::NUMBER::TIMESTAMP::DATE)                       AS valid_from,
      "DBT_VALID_TO"::NUMBER::TIMESTAMP::DATE                               AS valid_to
    FROM source
    WHERE deviation_from_comp_calc_cl IS NOT NULL

), deduplicated AS (

    SELECT DISTINCT   
      employee_number,
      original_value,
      IFF(CONTAINS(original_value,'%') = True,
          ROUND(deviation_from_comp_calc_cl/100::FLOAT, 4),
          ROUND(deviation_from_comp_calc_cl::FLOAT, 4))                     AS deviation_from_comp_calc,
    valid_from,
    valid_to
    FROM renamed
  
), final AS (

  SELECT
    employee_number,
    original_value,
    deviation_from_comp_calc,
    MIN(valid_from)                     AS valid_from,
    COALESCE(MAX(valid_to), '2020-05-20') AS valid_to
    ---last day we captured from this sheetload tab--
  FROM deduplicated
  GROUP BY 1, 2, 3

)

SELECT *
FROM final
