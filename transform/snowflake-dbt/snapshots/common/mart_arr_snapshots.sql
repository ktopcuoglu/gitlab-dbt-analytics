{% snapshot mart_arr_snapshots %}

    {{
        config(
          unique_key='primary_key',
          strategy='check',
          check_cols=[
              'shared_runners_minutes'
          ],
        )
    }}
    
    SELECT *
    FROM {{ ref('mart_arr') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY arr_month DESC) = 1

{% endsnapshot %}
