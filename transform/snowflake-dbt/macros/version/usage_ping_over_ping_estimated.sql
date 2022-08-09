{%- macro usage_ping_over_ping_estimated(all_time_event_metric_column, snapshot_month = 'snapshot_month') -%} 

    {%- set events_since_last_ping =  all_time_event_metric_column ~ '_since_last_ping' -%}
    {%- set event_first_month =  all_time_event_metric_column ~ '_first_ping_month' -%}
    {%- set event_last_month =  all_time_event_metric_column ~ '_last_ping_month' -%}
    {%- set events_smoothed =  all_time_event_metric_column ~ '_smoothed' -%}
    {%- set events_estimated_monthly_alias =  all_time_event_metric_column ~ '_estimated_monthly' -%}

    {{ all_time_event_metric_column }},
    {{ events_since_last_ping }},
      IFF({{ snapshot_month }} <= {{ event_first_month }}
            OR {{ snapshot_month }} > {{ event_last_month }},
          NULL, {{ events_smoothed }}
          )                                                   AS {{ events_estimated_monthly_alias }}

{%- endmacro -%}