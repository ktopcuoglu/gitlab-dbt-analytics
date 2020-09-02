{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_column_sum_min(
    'zuora', 
    'account',
    'mrr',
    13500000
) }}
