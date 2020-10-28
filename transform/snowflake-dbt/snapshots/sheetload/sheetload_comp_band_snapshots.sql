{% snapshot sheetload_comp_band_snapshots %}

    {{
        config(
          unique_key='employee_number',
          strategy='timestamp',
          updated_at='_UPDATED_AT',
        )
    }}

    SELECT *
    FROM {{ source('sheetload', 'comp_band') }}
    WHERE "Employee_ID" != ''

{% endsnapshot %}

