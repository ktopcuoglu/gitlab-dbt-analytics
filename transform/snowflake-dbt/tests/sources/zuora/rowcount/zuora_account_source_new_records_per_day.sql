{{ config({
    "tags": ["tdf","zuora"],
    "severity": "warn",
    })
}}

{{ source_new_rows_per_day(
    'zuora', 
    'account',
    'createddate',
    10,
    100,
    "date_trunc('day',createddate) >= '2020-01-01'"
) }}
