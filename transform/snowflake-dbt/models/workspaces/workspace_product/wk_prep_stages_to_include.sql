{{ config(
    tags=["mnpi_exception"]
) }}

WITH smau_only AS (

    SELECT DISTINCT stage_name
    FROM {{ ref('fct_monthly_usage_data') }}
    WHERE is_smau = TRUE

)

SELECT * 
FROM smau_only
