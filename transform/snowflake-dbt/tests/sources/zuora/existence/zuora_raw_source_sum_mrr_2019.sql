{{ config({
    "tags": ["tdf"]
    })
}}

{{ source_column_sum_min(
    'zuora', 
    'account',
    'mrr',
    3000000,
    "CREATEDDATE > '2019-01-01' and CREATEDDATE < '2020-01-01'"
) }}
