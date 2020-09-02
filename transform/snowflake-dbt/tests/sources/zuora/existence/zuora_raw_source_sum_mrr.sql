{{ config({
    "tags": ["tdf"]
    })
}}

{{ source_column_sum_min(
    'zuora', 
    'account',
    'mrr',
    13500000
) }}
