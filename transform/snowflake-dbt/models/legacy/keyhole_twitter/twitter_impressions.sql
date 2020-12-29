WITH source AS (

    SELECT *
    FROM {{ ref('twitter_impressions_source') }}

)


SELECT *
FROM source

