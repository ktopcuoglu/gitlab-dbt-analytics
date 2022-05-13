WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_visit_webpage_source') }}

)

SELECT *
FROM source
