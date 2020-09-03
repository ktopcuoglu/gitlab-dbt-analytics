{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_rowcount('zuora', 'subscription', 105000) }}
