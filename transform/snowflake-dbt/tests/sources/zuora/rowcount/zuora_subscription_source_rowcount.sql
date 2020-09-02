{{ config({
    "tags": ["tdf"]
    })
}}

{{ source_rowcount('zuora', 'subscription', 105000) }}
