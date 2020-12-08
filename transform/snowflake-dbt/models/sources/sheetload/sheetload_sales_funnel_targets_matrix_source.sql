WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_targets_matrix') }}

), renamed AS (

    SELECT
      kpi_name::VARCHAR                                   AS kpi_name,
      month::VARCHAR                                      AS month,
      opportunity_source::VARCHAR                         AS opportunity_source,
      order_type::VARCHAR                                 AS order_type,
      area::VARCHAR                                       AS area,
      allocated_target::NUMBER                            AS allocated_target,
      kpi_total::NUMBER                                   AS kpi_total,
      month_percentage::NUMBER                            AS month_percentage,
      opportunity_source_percentage::NUMBER               AS opportunity_source_percentage,
      order_type_percentage::NUMBER                       AS order_type_percentage,
      area_percentage::NUMBER                             AS area_percentage,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP  AS last_updated_at
    FROM source

)

SELECT *
FROM renamed

