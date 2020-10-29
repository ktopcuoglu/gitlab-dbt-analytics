{% snapshot sheetload_comp_band_snapshots %}

    {{
        config(
          unique_key='employee_number',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}

    SELECT *
    FROM {{ source('sheetload', 'comp_band') }}

{% endsnapshot %}

