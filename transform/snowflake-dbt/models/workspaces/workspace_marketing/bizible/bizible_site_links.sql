WITH source AS (

    SELECT
      *
    FROM {{ ref('bizible_site_links_source') }}

)

SELECT *
FROM source