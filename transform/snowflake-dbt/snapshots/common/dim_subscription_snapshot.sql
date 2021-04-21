{% snapshot dim_subscription_snapshot %}

    {{
        config(
          unique_key='dim_subscription_id',
          strategy='check',
          check_cols=['subscription_status', 'is_auto_renew']
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
