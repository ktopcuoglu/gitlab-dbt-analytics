{{ config({
        "materialized": "view"
        })
}}
    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_terraform_states_dedupe_source') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER               AS terraform_state_id,
      project_id::NUMBER       AS project_id,
      created_at::TIMESTAMP     AS created_at,
      updated_at::TIMESTAMP     AS updated_at,
      file_store::VARCHAR       AS file_store

    FROM source

)

SELECT  *
FROM renamed
