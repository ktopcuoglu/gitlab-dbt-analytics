{{ config({
    "tags": ["tdf","zuora"]
    })
}}

{{ source_column_existence(
    'zuora_stitch', 
    'account',
    ['name', 'id']
) }}
