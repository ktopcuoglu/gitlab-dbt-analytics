{{ config(
    tags=["product"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('usage_ping_metrics_source') }}
    QUALIFY MAX(uploaded_at) OVER() = uploaded_at

)

SELECT *
FROM source
