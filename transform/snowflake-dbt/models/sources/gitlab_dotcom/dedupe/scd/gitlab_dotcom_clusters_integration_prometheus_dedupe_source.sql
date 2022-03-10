WITH base AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'clusters_integration_prometheus') }}

)
{{ scd_latest_state(source=base) }}