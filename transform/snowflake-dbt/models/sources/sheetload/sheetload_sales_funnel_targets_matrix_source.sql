{{ config(
    tags=["mnpi"]
) }}

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
      REPLACE(allocated_target, ',', '')::FLOAT           AS allocated_target,
      user_segment::VARCHAR                               AS user_segment,
      user_geo::VARCHAR                                   AS user_geo,
      user_region::VARCHAR                                AS user_region,
      user_area::VARCHAR                                  AS user_area,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP  AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
