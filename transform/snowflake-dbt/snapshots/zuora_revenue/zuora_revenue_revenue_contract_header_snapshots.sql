{% snapshot zuora_rateplancharge_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_revenue_contract_header_snapshots') }}
    
{% endsnapshot %}
