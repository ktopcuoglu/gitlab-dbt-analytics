{% snapshot marts_arr_snapshots %}

    {{
        config(
          unique_key='primary_key',
          strategy='check',
          check_cols=['mrr', 'arr', 'quantity']
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('mart_arr')
            )
      }}
    FROM {{ ref('mart_arr') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY arr_month DESC) = 1

{% endsnapshot %}
