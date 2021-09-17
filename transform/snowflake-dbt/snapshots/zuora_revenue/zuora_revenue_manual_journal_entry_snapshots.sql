{% snapshot zuora_discountappliedmetrics_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updateddate',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_manual_journal_entry_snapshots') }}
    
{% endsnapshot %}
