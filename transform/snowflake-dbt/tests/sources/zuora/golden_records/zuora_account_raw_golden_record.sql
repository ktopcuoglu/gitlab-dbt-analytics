{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_golden_records_comparison('zuora', 'account') }}
