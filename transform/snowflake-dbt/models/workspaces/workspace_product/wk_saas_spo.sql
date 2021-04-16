WITH date_details AS (
  SELECT *
  FROM {{ ref('date_details') }}

), namespaces AS (
  
  SELECT DISTINCT
    namespace_id,
    namespace_type,
    owner_id,
    MIN(DATE(namespace_created_at)) AS namespace_created_at
  FROM {{ ref('gitlab_dotcom_namespaces') }}
  WHERE namespace_id = namespace_ultimate_parent_id
    AND namespace_is_internal = FALSE
  GROUP BY 1,2,3

)
            
, events AS (
  
  SELECT
    namespace_id,
    DATE(event_created_at) AS event_date,
    DATE_TRUNC('month',event_created_at) AS event_month,
    plan_name_at_event_date,
    user_id,
    stage_name,
    FIRST_VALUE(plan_name_at_event_date) OVER (PARTITION BY event_month, namespace_id ORDER BY event_date ASC) AS plan_name_at_reporting_month,
    FIRST_VALUE(plan_name_at_event_date) OVER (PARTITION BY namespace_id ORDER BY event_date ASC) AS plan_name_at_creation,
    COUNT(event_date) AS event_count
  FROM {{ ref('gitlab_dotcom_data_usage_events') }}
  WHERE
  --gitlab_dotcom_xmau_metrics. Join here 
    IFF(stage_name = 'secure', 
        event_name = 'secure_stage_ci_jobs',
          IFF(stage_name = 'plan', event_name IN ('issues','issue_notes'),
              IFF(event_name='container_scanning', 
                  TRUE, is_representative_of_stage = TRUE
                  )
           )
    )
    AND (DATE(event_created_at) BETWEEN '2021-03-11' AND '2021-03-15')
    AND stage_name NOT IN ('manage','monitor')
    --AND event_created_at > DATEADD('day', -28, date_details.date_day) AND DATEADD('month', 1, DATE_TRUNC('month',event_date))
    AND namespace_is_internal = FALSE
    AND days_since_namespace_creation >= 0
  GROUP BY 1,2,3,4,5,6
                                                                                  
)                                                             

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
    COUNT(DISTINCT event_date) AS stage_active_days
  FROM events
    JOIN date_details ON events.event_month = date_details.date_day
    JOIN namespaces ON namespaces.namespace_id = events.namespace_id
  WHERE event_date > DATEADD('day',-28, date_details.last_day_of_month)
  GROUP BY 1,2,3,4,5,6,7