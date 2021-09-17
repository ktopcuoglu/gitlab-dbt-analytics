{% snapshot zuora_product_rate_plan_charge_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_line_snapshots') }}

{% endsnapshot %}
