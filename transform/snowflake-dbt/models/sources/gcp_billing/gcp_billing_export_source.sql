{{ config({
    "materialized": "incremental",
    "unique_key": "primary_key",
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gcp_billing', 'gcp_billing_export_combined') }}
  {% if is_incremental() %}

  WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})

  {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY jsontext ORDER BY uploaded_at DESC) = 1

), renamed AS (

  SELECT
    flatten_export.value:billing_account_id::VARCHAR               AS billing_account_id,
    flatten_export.value:cost_type::VARCHAR                        AS cost_type,
    flatten_export.value:credits::VARIANT                          AS credits,
    flatten_export.value:currency::VARCHAR                         AS currency,
    flatten_export.value:currency_conversion_rate::FLOAT           AS currency_conversion_rate,
    flatten_export.value:export_time::TIMESTAMP                    AS export_time,
    TO_DATE(flatten_export.value:invoice:month::STRING,'YYYYMM')   AS invoice_month,
    flatten_export.value:labels::VARIANT                           AS labels,
    flatten_export.value:location:country::VARCHAR                 AS resource_country,
    flatten_export.value:location:location::VARCHAR                AS resource_location,
    flatten_export.value:location:region::VARCHAR                  AS resource_region,
    flatten_export.value:location:zone::VARCHAR                    AS resource_zone,
    flatten_export.value:project:ancestry_numbers::VARCHAR         AS folder_id,
    flatten_export.value:project:id::VARCHAR                       AS project_id,
    flatten_export.value:project:labels::VARIANT                   AS project_labels,
    flatten_export.value:project:name::VARCHAR                     AS project_name,
    flatten_export.value:service:id::VARCHAR                       AS service_id,
    flatten_export.value:service:description::VARCHAR              AS service_description,
    flatten_export.value:sku:id::VARCHAR                           AS sku_id,
    flatten_export.value:sku:description::VARCHAR                  AS sku_description,
    flatten_export.value:system_labels::VARIANT                    AS system_labels,
    flatten_export.value:usage:pricing_unit::VARCHAR               AS pricing_unit,
    flatten_export.value:usage:unit::VARCHAR                       AS usage_unit,
    flatten_export.value:usage_start_time::TIMESTAMP               AS usage_start_time,
    flatten_export.value:usage_end_time::TIMESTAMP                 AS usage_end_time,
    {{ dbt_utils.surrogate_key([
        'flatten_export.value:billing_account_id',
        'flatten_export.value:cost_type',
        'flatten_export.value:credits',
        'flatten_export.value:currency',
        'flatten_export.value:currency_conversion_rate',
        'flatten_export.value:export_time'
        'flatten_export.value:invoice:month',
        'flatten_export.value:labels',
        'flatten_export.value:location:country',
        'flatten_export.value:location:location',
        'flatten_export.value:location:region',
        'flatten_export.value:location:zone',
        'flatten_export.value:project:ancestry_numbers',
        'flatten_export.value:project:id',
        'flatten_export.value:project:name',
        'flatten_export.value:project:labels',
        'flatten_export.value:service:id',
        'flatten_export.value:service:description',
        'flatten_export.value:sku:id',
        'flatten_export.value:sku:description',
        'flatten_export.value:system_labels',
        'flatten_export.value:usage:pricing_unit',
        'flatten_export.value:usage:unit',
        'flatten_export.value:usage_start_time',
        'flatten_export.value:usage_end_time'] ) }}                 AS primary_key,
    source.uploaded_at                                              AS uploaded_at,
    SUM(flatten_export.value:cost::FLOAT)                           AS cost,
    SUM(flatten_export.value:usage:amount::FLOAT)                   AS usage_amount,
    SUM(flatten_export.value:usage:amount_in_pricing_units::FLOAT ) AS usage_amount_in_pricing_units

  FROM source,
  TABLE(FLATTEN(source.jsontext)) flatten_export
  {{ dbt_utils.group_by(n=27) }}

)


SELECT *
FROM renamed
