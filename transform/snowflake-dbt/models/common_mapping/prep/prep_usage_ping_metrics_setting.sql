{{ simple_cte([('usage_ping_metrics_latest', 'usage_ping_metrics_latest')]) }}

SELECT *
FROM usage_ping_metrics_latest
WHERE time_frame = 'none'
