WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_experiments_dedupe_source') }}

), renamed AS (

    SELECT
      id::NUMBER    AS experiment_id,
      name::VARCHAR AS experiment_name
    FROM source
    
)

SELECT *
FROM renamed
