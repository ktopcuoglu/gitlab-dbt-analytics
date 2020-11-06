WITH sprints_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_sprints_source') }}

), filtered_sprints AS (

    SELECT *
    FROM sprints_source
    WHERE group_id = 9970 OR group_id = 6543

)

SELECT *
FROM filtered_sprints