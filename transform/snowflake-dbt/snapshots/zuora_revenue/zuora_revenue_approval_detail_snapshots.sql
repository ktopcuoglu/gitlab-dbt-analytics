{% snapshot zuora_amendment_snapshots %}

    {{
        config(
          strategy='timestamp',
          unique_key='rc_id',
          updated_at='updt_dt',
        )
    }}

    SELECT *
    FROM {{ source('zuora_revenue', 'zuora_revenue_approval_detail') }}

{% endsnapshot %}
