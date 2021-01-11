{{ config({
    "materialized": "table"
    })
}}

WITH usage_data AS (

    SELECT * 
    FROM {{ ref('prep_usage_ping') }}

)
