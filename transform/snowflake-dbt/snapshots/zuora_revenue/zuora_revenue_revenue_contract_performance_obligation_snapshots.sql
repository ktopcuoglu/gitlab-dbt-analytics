{% snapshot zuora_revenue_revenue_contract_performance_obligation_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='rc_pob_id',
          updated_at='incr_updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_performance_obligation') }}
    QUALIFY RANK() OVER (PARTITION BY id ORDER BY incr_updt_dt DESC) = 1

{% endsnapshot %}
