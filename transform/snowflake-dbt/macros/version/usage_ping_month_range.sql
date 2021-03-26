{%- macro usage_ping_month_range(all_time_event_metric_column, month_column = 'snapshot_month', group_by_column = 'dim_subscription_id') -%} 

    {%- set first_month_alias =  all_time_event_metric_column ~ '_first_ping_month' -%}
    {%- set last_month_alias =  all_time_event_metric_column ~ '_last_ping_month' -%}

    MIN(IFF({{ all_time_event_metric_column }} IS NOT NULL,
            {{ month_column }},
            NULL)
        ) OVER (PARTITION BY {{ group_by_column }})             AS {{ first_month_alias }},
     MAX(IFF({{ all_time_event_metric_column }} IS NOT NULL,
            {{ month_column }},
            NULL)
        ) OVER (PARTITION BY {{ group_by_column }})             AS {{ last_month_alias }}

{%- endmacro -%}