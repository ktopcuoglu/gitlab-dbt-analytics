{% snapshot dim_ping_metric_daily_snapshot %}
    {{
        config(
          unique_key='ping_metric_id',
          strategy='check',
          check_cols=[
              'data_source',
              'product_category',
              'group_name',
              'section_name',
              'stage_name',
              'milestone',
              'skip_validation'
              'metrics_status'
              'tier',
              'time_frame',
              'value_type',
              'is_gmau',
              'is_smau',
              'is_paid_gmau',
              'is_umau',
          ],
         )
    }}
    
    SELECT *
    FROM {{ ref('dim_ping_metric') }}

{% endsnapshot %}
