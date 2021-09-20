{% snapshot zuora_product_rate_plan_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_schedule_deleted_snapshots') }}

{% endsnapshot %}
