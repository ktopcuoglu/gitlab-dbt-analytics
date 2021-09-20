{% snapshot zuora_product_rate_plan_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='schd_id',
          updated_at='incr_updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_schedule_deleted') }}

{% endsnapshot %}
