WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_page_views_source') }}
    FROM {{ ref('bizible_page_views_source') }}

)

SELECT *
FROM source