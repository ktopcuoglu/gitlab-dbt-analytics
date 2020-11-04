{% snapshot sheetload_comp_band_snapshots %}

    {{
        config(
          unique_key='employee_number',
          strategy='timestamp',
          updated_at='updated_at',
        )
    }}

    SELECT 
      employee_number,
      percent_over_top_end_of_band,
      DATEADD('sec', _updated_at, '1970-01-01')::TIMESTAMP AS updated_at
    FROM {{ source('sheetload', 'comp_band') }}

{% endsnapshot %}

