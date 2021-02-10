    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_prometheus_alerts_dedupe_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                                     AS prometheus_alert_id,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at,
      project_id::NUMBER                             AS project_id
    FROM source

)

SELECT *
FROM renamed
