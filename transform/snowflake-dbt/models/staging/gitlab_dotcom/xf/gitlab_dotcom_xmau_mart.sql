WITH epic_interaction AS (

    SELECT *
    FROM {{ ref('gitlab_dotocm_daily_usage_data_events') }}
    WHERE event_name IN ('epics', 'epic_notes')

)

, issue_interaction AS (

    SELECT *
    FROM {{ ref('gitlab_dotocm_daily_usage_data_events') }}
    WHERE event_name IN ('issues', 'issue_notes')

)

, merge_request_interaction AS (

    SELECT *
    FROM {{ ref('gitlab_dotocm_daily_usage_data_events') }}
    WHERE event_name IN ('merge_requests', 'merge_request_notes')
    
)

, unioned AS (

    SELECT *
    FROM epic_interaction

    UNION 

    SELECT *
    FROM issue_interaction

    UNION

    SELECT *
    FROM merge_request_interaction

)

, skeleton AS (

    SELECT *
    FROM {{ ref('date_details') }}

)

, joined AS (

    SELECT *
    FROM date_details
    LEFT JOIN unioned
        ON date_details.date

)
