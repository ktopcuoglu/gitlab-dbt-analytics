WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_boards_epic_boards_source') }}

)

SELECT *
FROM source
