{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_new_rows_per_day(
    'zuora', 
    'subscription',
    'createddate',
    50,
    200,
    "date_trunc('day',createddate) >= '2020-01-01'"
) }}
