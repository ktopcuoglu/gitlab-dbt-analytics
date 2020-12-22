WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_platform_metrics_source') }}

)

SELECT *
FROM source
