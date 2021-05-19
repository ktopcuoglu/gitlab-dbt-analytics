{%- macro usage_ping_over_ping_smoothed(all_time_event_metric_column, partition_by_column = 'dim_subscription_id', order_by_column = 'snapshot_month', days_since_last_ping = 'days_since_last_ping', days_in_month = 'days_in_month_count') -%} 

    {%- set events_since_last_ping =  all_time_event_metric_column ~ '_since_last_ping' -%}
    {%- set events_per_day_alias =  all_time_event_metric_column ~ '_estimated_daily' -%}
    {%- set events_smoothed_alias =  all_time_event_metric_column ~ '_smoothed' -%}

    {{ all_time_event_metric_column }},
    {{ events_since_last_ping }},
    FIRST_VALUE({{ events_since_last_ping }} / {{ days_since_last_ping }})
        IGNORE NULLS OVER (
          PARTITION BY {{ partition_by_column }}
          ORDER BY {{ order_by_column }}
          ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )                                                                   AS {{ events_per_day_alias }},
    ({{ events_per_day_alias }} * {{ days_in_month }})::INT                 AS {{ events_smoothed_alias }}

{%- endmacro -%}