{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_table_existence(
    'zuora_stitch', 
    ['account', 'subscription', 'rateplancharge']
) }}
