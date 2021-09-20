{% snapshot zuora_invoiceitem_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='rc_hold_id',
          updated_at='incr_updt_dt',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_hold') }}
    
{% endsnapshot %}
