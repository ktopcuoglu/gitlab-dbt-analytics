WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_pipeline_chat_data_source') }}

)

SELECT *
FROM source
