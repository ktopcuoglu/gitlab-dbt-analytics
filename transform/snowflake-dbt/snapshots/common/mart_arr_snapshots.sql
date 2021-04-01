{% snapshot mart_arr_snapshots %}

    {{
        config(
          unique_key='primary_key',
          strategy='timestamp',
          updated_at='dbt_created_at',
        )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('mart_arr'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('mart_arr') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY arr_month DESC) = 1

{% endsnapshot %}
