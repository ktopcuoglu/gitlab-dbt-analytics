WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY epic_id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ ref('gitlab_dotcom_epic_metrics_dedupe_source') }}

), renamed AS (

    SELECT
      epic_id::NUMBER                    AS epic_id,
      created_at::TIMESTAMP               AS created_at,
      updated_at::TIMESTAMP               AS updated_at

    FROM source

)


SELECT *
FROM renamed
