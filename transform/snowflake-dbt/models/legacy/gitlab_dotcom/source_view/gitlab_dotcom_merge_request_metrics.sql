WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_request_metrics_source') }}

)

SELECT *
FROM source
