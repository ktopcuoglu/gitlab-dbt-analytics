{% snapshot mart_arr_snapshot %}

    {{
        config(
          unique_key='primary_key',
          strategy='timestamp',
          -- Using dbt updated at field as we want a new set of data everyday.
          updated_at='dbt_updated_at'
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
