WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_gitlab_subscription_histories_source') }}

)

SELECT *
FROM source
