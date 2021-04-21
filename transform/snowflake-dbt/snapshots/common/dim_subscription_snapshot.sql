{% snapshot dim_subscription_snapshot %}

    {{
        config(
          unique_key='dim_subscription_id',
          strategy='timestamp',
          -- Using dbt updated at field as we want a new set of data everyday.
          updated_at='dbt_updated_at'
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('dim_subscription'),
            except=['DBT_UPDATED_AT', 'DBT_CREATED_AT']
            )
      }}
    FROM {{ ref('dim_subscription') }}

{% endsnapshot %}
