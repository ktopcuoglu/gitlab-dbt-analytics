{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ model_rowcount('zuora_account_source', 26000) }}
