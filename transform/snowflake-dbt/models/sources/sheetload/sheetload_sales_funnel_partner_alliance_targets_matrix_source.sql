WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_funnel_partner_alliance_targets_matrix') }}

), renamed AS (

    SELECT
      kpi_name::VARCHAR                                   AS kpi_name,
      month::VARCHAR                                      AS month,
      partner_engagement_type::VARCHAR                    AS partner_engagement_type,
      alliance_partner::VARCHAR                           AS alliance_partner,
      order_type::VARCHAR                                 AS alliance_partner,
      area:VARCHAR                                        AS area,
      REPLACE(allocated_target, ',', '')::FLOAT           AS allocated_target,
      TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP  AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
