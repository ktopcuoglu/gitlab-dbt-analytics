{{ config(
    tags=["product"]
) }}

WITH source AS (

    SELECT *
    FROM {{ ref('usage_ping_metrics_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY metrics_path ORDER BY snapshot_date DESC) =1 


)

SELECT *
FROM source
