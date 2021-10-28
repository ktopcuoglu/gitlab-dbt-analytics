WITH gitlab_dotcom_routes AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_routes_dedupe_source') }}

)

SELECT
  id::NUMBER                    AS route_id,
  source_id::NUMBER             AS source_id,
  source_type::VARCHAR          AS source_type,
  path::VARCHAR                 AS path
FROM gitlab_dotcom_routes
