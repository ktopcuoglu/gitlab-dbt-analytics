WITH sprints_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_sprints_source') }}

), internal_groups AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_groups_xf') }}
    WHERE group_is_internal

), filtered_sprints AS (

    SELECT *
    FROM sprints_source
    WHERE EXISTS (
        SELECT 1
        FROM internal_groups
        WHERE sprints_source.group_id = internal_groups.group_id
    ) 

)

SELECT *
FROM filtered_sprints