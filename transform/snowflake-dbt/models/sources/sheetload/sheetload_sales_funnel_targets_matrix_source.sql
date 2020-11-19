WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_matrix') }}

), renamed AS (

    SELECT
      kpi_name::VARCHAR                                   AS kpi_name,
      month::VARCHAR                                      AS month,
      sales_segment::VARCHAR                              AS sales_segment,
      opportunity_source::VARCHAR                         AS opportunity_source,
      order_type::VARCHAR                                 AS order_type,
      region::VARCHAR                                     AS region,
      area::VARCHAR                                       AS area,
      allocated_target::NUMBER                            AS allocated_target,
      kpi_total::NUMBER                                   AS kpi_total,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP  AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
