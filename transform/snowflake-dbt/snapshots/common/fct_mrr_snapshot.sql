{% snapshot fct_mrr_snapshot %}

    {{
        config(
          unique_key='mrr_id',
          strategy='timestamp',
          -- Using dbt updated at field as we want a new set of data everyday.
          updated_at='dbt_updated_at'
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('fct_mrr'),
            except=['DBT_UPDATED_AT', 'DBT_CREATED_AT']
            )
      }}
    FROM {{ ref('fct_mrr') }}

{% endsnapshot %}
