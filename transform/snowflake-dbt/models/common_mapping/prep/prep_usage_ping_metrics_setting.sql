{{ simple_cte([('dim_usage_ping_metric', 'dim_usage_ping_metric')]) }}

SELECT *
FROM dim_usage_ping_metric
WHERE time_frame = 'none'
