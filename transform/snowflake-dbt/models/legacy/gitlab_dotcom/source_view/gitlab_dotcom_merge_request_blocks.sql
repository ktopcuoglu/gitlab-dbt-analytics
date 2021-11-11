WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_request_blocks_source') }}

)

SELECT *
FROM source
