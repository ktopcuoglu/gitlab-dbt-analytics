{%- macro usage_ping_over_ping_difference(all_time_event_metric_column, partion_by_columns = ['dim_subscription_id' ,'uuid', 'hostname'], order_by_column = 'snapshot_month') -%} 

    {%- set ping_over_ping_alias =  all_time_event_metric_column ~ '_since_last_ping' -%}

    {{ all_time_event_metric_column }},
    {{ all_time_event_metric_column }} - LAG({{ all_time_event_metric_column }})
      IGNORE NULLS OVER (
        PARTITION BY
          {{ partion_by_columns[0] }},
          {{ partion_by_columns[1] }},
          {{ partion_by_columns[2] }}
        ORDER BY {{ order_by_column }}
      )                                                                         AS {{ ping_over_ping_alias }}

{%- endmacro -%}