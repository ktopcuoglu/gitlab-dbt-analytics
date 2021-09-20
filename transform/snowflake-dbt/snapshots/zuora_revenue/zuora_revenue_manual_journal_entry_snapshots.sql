{% snapshot zuora_discountappliedmetrics_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updt_dt',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_manual_journal_entry_snapshots') }}
    
{% endsnapshot %}
