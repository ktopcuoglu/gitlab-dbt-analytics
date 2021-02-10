WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_deployment_merge_requests_dedupe_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY deployment_merge_request_id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      deployment_id::NUMBER                           AS deployment_id,
      merge_request_id::NUMBER                        AS merge_request_id,
      MD5(deployment_merge_request_id::VARCHAR)        AS deployment_merge_request_id
    FROM source

)

SELECT *
FROM renamed
