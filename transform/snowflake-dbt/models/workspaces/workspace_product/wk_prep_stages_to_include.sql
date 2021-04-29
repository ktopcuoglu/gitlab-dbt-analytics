WITH smau_only AS (

    SELECT DISTINCT stage_name
    FROM {{ ref('monthly_usage_data') }}
    WHERE is_smau

)

SELECT * 
FROM smau_only
