{%- macro usage_ping_over_ping_smoothed(all_time_event_metric_column, partion_by_columns = ['dim_subscription_id' ,'uuid', 'hostname'], order_by_column = 'snapshot_month', days_since_last_ping = 'days_since_last_ping', days_in_month = 'days_in_month_count') -%} 

    {%- set events_since_last_ping =  all_time_event_metric_column ~ '_since_last_ping' -%}
    {%- set events_per_day_alias =  all_time_event_metric_column ~ '_estimated_daily' -%}
    {%- set events_smoothed_alias =  all_time_event_metric_column ~ '_smoothed' -%}

    {{ all_time_event_metric_column }},
    {{ events_since_last_ping }},
    FIRST_VALUE({{ events_since_last_ping }} / {{ days_since_last_ping }})
        IGNORE NULLS OVER (
          PARTITION BY
            {{ partion_by_columns[0] }},
            {{ partion_by_columns[1] }},
            {{ partion_by_columns[2] }}
          ORDER BY {{ order_by_column }}
          ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )                                                                   AS {{ events_per_day_alias }},
    ({{ events_per_day_alias }} * {{ days_in_month }})::INT                 AS {{ events_smoothed_alias }}

{%- endmacro -%}