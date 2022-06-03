{% snapshot workday_employee_mapping_snapshots %}

    {{
        config(
          unique_key='employee_id',
          strategy='timestamp',
          updated_at='_fivetran_synced',
        )
    }}
    
    SELECT * 
    FROM {{ source('workday','employee_mapping') }}
    
{% endsnapshot %}