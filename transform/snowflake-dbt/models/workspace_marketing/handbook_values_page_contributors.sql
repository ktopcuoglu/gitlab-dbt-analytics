WITH source AS (

    SELECT *
    FROM {{ ref('handbook_values_page_contributors')}}

)

SELECT *
FROM source
