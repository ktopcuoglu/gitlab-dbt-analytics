WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_page_views_source') }}

)

SELECT *
FROM source