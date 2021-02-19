WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sdr_count_snapshot') }}

), renamed as (

    SELECT
      quarter::VARCHAR                            AS fiscal_quarter,
      sales_segment::VARCHAR                      AS sales_segment,
      sdr_count::NUMERIC                          AS sdr_count
      
    FROM source
)

SELECT *
FROM renamed
