{% snapshot zuora_revenue_manual_journal_entry_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='je_line_id',
          updated_at='incr_updt_dt',
        )
    }}
    
    SELECT * 
    FROM {{ source('zuora_revenue','zuora_revenue_manual_journal_entry') }}
    
{% endsnapshot %}
