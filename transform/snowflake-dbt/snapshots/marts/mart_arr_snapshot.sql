{% snapshot mart_arr_snapshot %}

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
            from=ref('mart_arr'),
            except=['DBT_UPDATED_AT', 'DBT_CREATED_AT']
            )
      }}
    FROM {{ ref('mart_arr') }}

{% endsnapshot %}
