WITH source AS (

    SELECT *
    FROM {{ ref('monitor_recom_dimension_tracking_source') }}

)

SELECT *
FROM source
