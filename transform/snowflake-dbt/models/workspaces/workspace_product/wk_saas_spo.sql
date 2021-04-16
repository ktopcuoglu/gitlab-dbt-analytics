{{ config({
    "materialized": "table"
    })
}}

WITH date_details AS (
    SELECT *
    FROM {{ ref('date_details') }}
  
), blocked_users AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_users_blocked_xf') }}

), all_namespaces AS (
  
    SELECT DISTINCT
        namespace_id,
        namespace_type,
        creator_id,
        MIN(DATE(namespace_created_at)) AS namespace_created_at
    FROM {{ ref('gitlab_dotcom_namespaces_xf') }}
    WHERE namespace_id = namespace_ultimate_parent_id
        AND namespace_is_internal = FALSE
    GROUP BY 1,2,3

)

, namespaces AS (
  
    SELECT all_namespaces.*
    FROM all_namespaces
        LEFT JOIN blocked_users ON all_namespaces.creator_id = blocked_users.user_id
    WHERE blocked_users.user_id IS NULL
  
)
            
, all_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_usage_data_events') }}
  
)

, metrics AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_xmau_metrics') }}
  
), events AS (
  
    SELECT
        namespace_id,
        DATE(event_created_at) AS event_date,
        DATE_TRUNC('month',event_created_at) AS event_month,
        plan_name_at_event_date,
        user_id,
        all_events.stage_name,
        IFF(all_events.stage_name = 'manage',user_id,NULL) AS umau,
        FIRST_VALUE(plan_name_at_event_date) OVER (PARTITION BY event_month, namespace_id ORDER BY event_date ASC) AS plan_name_at_reporting_month,
        FIRST_VALUE(plan_name_at_event_date) OVER (PARTITION BY namespace_id ORDER BY event_date ASC) AS plan_name_at_creation,
        COUNT(event_date) AS event_count
    FROM all_events
        JOIN metrics ON all_events.event_name = metrics.events_to_include
    WHERE
        (metrics.smau = TRUE OR metrics.is_umau = TRUE)
        AND all_events.stage_name != 'monitor'
        AND namespace_is_internal = FALSE
        AND days_since_namespace_creation >= 0
    GROUP BY 1,2,3,4,5,6
                                                                                  
), joined AS (                                               

    SELECT
        'SaaS' AS delivery, 
        namespaces.namespace_id AS organization_id,
        namespace_type AS organization_type,
        DATE(namespace_created_at) AS organization_creation_date,
        first_day_of_month,
        stage_name,
        plan_name_at_reporting_month,
        IFF(plan_name_at_reporting_month IN ('free','trial'),'free','paid') AS plan_is_paid,
        SUM(event_count) AS monthly_stage_events,
        COUNT(DISTINCT user_id) AS monthly_stage_users,
        COUNT(DISTINCT event_date) AS stage_active_days,
        COUNT(DISTINCT umau) AS umau_stage,
        SUM(umau_stage) OVER (PARTITION BY organization_id, first_day_of_month, plan_name_at_reporting_month) AS umau
    FROM events
        JOIN date_details ON events.event_month = date_details.date_day
        JOIN namespaces ON namespaces.namespace_id = events.namespace_id
    WHERE event_date > DATEADD('day',-28, date_details.last_day_of_month)
    GROUP BY 1,2,3,4,5,6,7
  
)

    SELECT
        delivery,
        organization_id,
        organization_type,
        organization_creation_date,
        plan_name_at_reporting_month,
        plan_is_paid,
        first_day_of_month,
        stage_name,
        monthly_stage_users,
        umau
    FROM joined
    WHERE stage_name != 'manage'
