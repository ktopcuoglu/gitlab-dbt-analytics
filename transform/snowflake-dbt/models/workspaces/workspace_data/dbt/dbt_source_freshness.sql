WITH dbt_source AS (

    SELECT *
    FROM {{ ref('dbt_source_freshness_results_source') }}

)

SELECT * 
FROM dbt_source
