-- SELECT 
--   delivery,
--   {{dbt_utils.surrogate_key('instance_id',  'host_id')}} AS installation_id,
--   stage_name,
--   reporting_month,

-- FROM {{ ref('monthly_usage_data') }}
-- WHERE is_smau AND delivery = 'Self-Managed'


WITH date_skeleton AS (

    SELECT 
      DISTINCT first_day_of_month AS reporting_month, 
      last_day_of_month
    FROM {{ ref('dim_date') }}
    WHERE date_day = last_day_of_month
        AND last_day_of_month < CURRENT_DATE()

)
, gitlab_dotcom_xmau_metrics AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_xmau_metrics') }}

)

, events AS (

    SELECT 
      user_id,
      namespace_id,
      event_date,
      plan_name_at_event_date,
      plan_id_at_event_date,
      namespace_is_internal,
      DATE_TRUNC('month', namespace_created_at)  AS namespace_created_month,
      xmau.event_name                            AS event_name,
      xmau.stage_name                            AS stage_name,
      xmau.smau::BOOLEAN                         AS is_smau,
      xmau.group_name                            AS group_name,
      xmau.gmau::BOOLEAN                         AS is_gmau,
      xmau.section_name::VARCHAR                 AS section_name,
      xmau.is_umau::BOOLEAN                      AS is_umau
    FROM {{ ref('gitlab_dotcom_daily_usage_data_events') }} AS events
    INNER JOIN gitlab_dotcom_xmau_metrics AS xmau
      ON events.event_name = xmau.events_to_include
        AND (xmau.is_smau OR xmau.is_umau)


), joined AS (

    SELECT 
      reporting_month,
      stage_name,
      is_smau,
      section_name,
      is_umau,
      namespace_id,
      namespace_created_month,
      COUNT(DISTINCT user_id)                            AS total_user_count,
      IFF(total_user_count >  0, TRUE, FALSE)            AS is_active
    FROM skeleton
    LEFT JOIN events
        ON event_date BETWEEN DATEADD('days', -28, last_day_of_month) AND last_day_of_month
    {{ dbt_utils.group_by(n=7) }}

)

SELECT *
FROM joined
