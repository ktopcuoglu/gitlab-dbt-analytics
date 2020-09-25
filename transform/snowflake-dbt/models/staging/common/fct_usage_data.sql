WITH 28_day_metrics AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_metric_28_days') }}

), all_time_metrics AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_metric_all_time') }}

), unioned AS (

    SELECT *
    FROM all_time_metrics

    UNION ALL

    SELECT *
    FROM 28_day_metrics


)

SELECT *
FROM unioned