{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ model_rowcount(
    'zuora_subscription_source', 
    18489, 
    "auto_renew = 'TRUE' and created_date > '2019-01-01' and created_date < '2020-01-01'"
) }}
