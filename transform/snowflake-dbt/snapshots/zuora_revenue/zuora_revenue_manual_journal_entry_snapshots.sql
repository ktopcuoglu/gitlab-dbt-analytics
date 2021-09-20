{% snapshot zuora_discountappliedmetrics_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='je_line_id',
          updated_at='je_line_updt_dt',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_manual_journal_entry') }}
    
{% endsnapshot %}
