{% snapshot fct_retention_snapshot %}
-- Using dbt updated at field as we want a new set of data everyday.
    {{
        config(
          unique_key='fct_retention_id',
          strategy='timestamp',
          updated_at='dbt_created_at'
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('fct_retention'),
            except=['DBT_UPDATED_AT']
            )
      }}
    FROM {{ ref('fct_retention') }}

{% endsnapshot %}
