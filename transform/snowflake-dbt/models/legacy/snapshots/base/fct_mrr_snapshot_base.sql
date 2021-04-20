{{ config({
    "alias": "gitlab_dotcom_application_settings_snapshots"
    })
}}

WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'fct_mrr_snapshots') }}
    
)

SELECT *
FROM base
