WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_pipeline_chat_data_dedupe_source') }}

), renamed AS (

    SELECT 
      pipeline_id::NUMBER  AS ci_pipeline_id,
      chat_name_id::NUMBER AS chat_name_id,
      response_url          AS response_url

    FROM source

)


SELECT *
FROM renamed
