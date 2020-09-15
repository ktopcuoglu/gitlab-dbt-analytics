{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ model_rowcount('zuora_subscription_source', 105000) }}
