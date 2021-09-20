{% snapshot zuora_revenue_schedule_item_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue', 'revenue_schedule_item') }}

{% endsnapshot %}
