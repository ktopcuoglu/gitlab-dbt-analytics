{{ config({
    "tags": ["tdf","zuora"],
    "severity": "warn",
    })
}}

{{ model_new_rows_per_day(
    'zuora_account_source', 
    'created_date',
    10,
    100,
    "date_trunc('day',created_date) >= '2020-01-01'"
) }}
