{% snapshot fcts_mrr_snapshots %}

    {{
        config(
          unique_key='mrr_id',
          strategy='check',
          check_cols=['mrr', 'arr', 'quantity']
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY mrr_id ORDER BY arr_month DESC) = 1

{% endsnapshot %}
