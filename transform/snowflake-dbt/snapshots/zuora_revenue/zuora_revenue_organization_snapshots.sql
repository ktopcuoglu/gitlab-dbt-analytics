{% snapshot zuora_rateplan_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_organization_snapshots') }}
    
{% endsnapshot %}
