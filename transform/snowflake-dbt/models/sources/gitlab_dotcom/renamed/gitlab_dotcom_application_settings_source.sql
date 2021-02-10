WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_application_settings_dedupe_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (
  
    SELECT
      id::NUMBER                          AS application_settings_id,
      shared_runners_minutes::NUMBER      AS shared_runners_minutes
    FROM source
    
)

SELECT * 
FROM renamed
