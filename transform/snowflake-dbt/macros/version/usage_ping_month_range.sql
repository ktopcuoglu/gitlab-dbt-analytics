{%- macro usage_ping_month_range(all_time_event_metric_column, month_column = 'snapshot_month', partion_by_columns = ['dim_subscription_id' ,'uuid', 'hostname']) -%} 

    {%- set first_month_alias =  all_time_event_metric_column ~ '_first_ping_month' -%}
    {%- set last_month_alias =  all_time_event_metric_column ~ '_last_ping_month' -%}

    MIN(IFF({{ all_time_event_metric_column }} IS NOT NULL,
            {{ month_column }},
            NULL)
        ) OVER (
            PARTITION BY
              {{ partion_by_columns[0] }},
              {{ partion_by_columns[1] }},
              {{ partion_by_columns[2] }}
        )                                                       AS {{ first_month_alias }},
    MAX(IFF({{ all_time_event_metric_column }} IS NOT NULL,
            {{ month_column }},
            NULL)
        ) OVER (
            PARTITION BY
              {{ partion_by_columns[0] }},
              {{ partion_by_columns[1] }},
              {{ partion_by_columns[2] }}
        )                                                       AS {{ last_month_alias }}

{%- endmacro -%}