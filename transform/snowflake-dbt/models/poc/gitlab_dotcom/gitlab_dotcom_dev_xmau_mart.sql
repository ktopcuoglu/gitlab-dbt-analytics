WITH skeleton AS (

    SELECT 
      DISTINCT first_day_of_month, 
      last_day_of_month
    FROM {{ ref('date_details') }}
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
      xmau.event_name            AS event_name,
      xmau.stage_name            AS stage_name,
      xmau.smau::BOOLEAN         AS is_smau,
      xmau.group_name            AS group_name,
      xmau.gmau::BOOLEAN         AS is_gmau,
      xmau.section_name::VARCHAR AS section_name,
      xmau.is_umau::BOOLEAN      AS is_umau
    FROM {{ ref('gitlab_dotcom_daily_usage_data_events') }} AS events
    INNER JOIN gitlab_dotcom_xmau_metrics AS xmau
      ON events.event_name = xmau.events_to_include

)

, joined AS (

    SELECT 
      first_day_of_month,
      event_name,
      stage_name,
      is_smau,
      group_name,
      is_gmau,
      section_name,
      is_umau,
      COUNT(DISTINCT user_id)                                                                           AS total_user_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date='free',user_id, NULL))                                 AS free_user_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date IN ('bronze', 'silver', 'gold'), user_id, NULL))       AS paid_user_count,
      COUNT(DISTINCT namespace_id)                                                                      AS total_namespace_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date='free',namespace_id, NULL))                            AS free_namespace_count,
      COUNT(DISTINCT IFF(plan_name_at_event_date IN ('bronze', 'silver', 'gold'), namespace_id, NULL))  AS paid_namespace_count
    FROM skeleton
    LEFT JOIN events
        ON event_date BETWEEN DATEADD('days', -28, last_day_of_month) AND last_day_of_month
    {{ dbt_utils.group_by(n=8) }}

)

SELECT *
FROM joined
