WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'clari_export_forecast_net_iacv') }}

)

SELECT
  domain::VARCHAR               AS domain,
  classification::VARACHAR      AS classification
FROM source
