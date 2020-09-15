{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_golden_records_unchanged('zuora', 'account') }}
