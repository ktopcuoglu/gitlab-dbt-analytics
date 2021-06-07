{% snapshot fct_mrr_snapshot %}
-- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='mrr_id',
          strategy='timestamp',
          updated_at='dbt_created_at'
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('fct_mrr'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('fct_mrr') }}

{% endsnapshot %}
