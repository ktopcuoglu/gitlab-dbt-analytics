{{ config({
    "materialized": "incremental",
    "unique_key" : "primary_key"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gcp_billing', 'gcp_billing_export_combined') }}
  {% if is_incremental() %}

  WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})

  {% endif %}

), flattened AS (

    SELECT
      flatten_export.value['billing_account_id']::VARCHAR                 AS billing_account_id,
      flatten_export.value['cost']::FLOAT                                 AS cost,
      flatten_export.value['cost_type']::VARCHAR                          AS cost_type,
      flatten_export.value['credits']::VARIANT                            AS credits,
      flatten_export.value['currency']::VARCHAR                           AS currency,
      flatten_export.value['currency_conversion_rate']::FLOAT             AS currency_conversion_rate,
      flatten_export.value['export_time']::TIMESTAMP                      AS export_time,
      TO_DATE(flatten_export.value['invoice']['month']::STRING,'YYYYMM')  AS invoice_month,
      flatten_export.value['labels']::VARIANT                             AS labels,
      flatten_export.value['location']['country']::VARCHAR                AS resource_country,
      flatten_export.value['location']['location']::VARCHAR               AS resource_location,
      flatten_export.value['location']['region']::VARCHAR                 AS resource_region,
      flatten_export.value['location']['zone']::VARCHAR                   AS resource_zone,
      flatten_export.value['project']['ancestry_numbers']::VARCHAR        AS folder_id,
      flatten_export.value['project']['id']::VARCHAR                      AS project_id,
      flatten_export.value['project']['labels']::VARIANT                  AS project_labels,
      flatten_export.value['project']['name']::VARCHAR                    AS project_name,
      flatten_export.value['service']['id']::VARCHAR                      AS service_id,
      flatten_export.value['service']['description']::VARCHAR             AS service_description,
      flatten_export.value['sku']['id']::VARCHAR                          AS sku_id,
      flatten_export.value['sku']['description']::VARCHAR                 AS sku_description,
      flatten_export.value['system_labels']::VARIANT                      AS system_labels,
      flatten_export.value['usage']['pricing_unit']::VARCHAR              AS pricing_unit,
      flatten_export.value['usage']['amount']::FLOAT                      AS usage_amount,
      flatten_export.value['usage']['amount_in_pricing_units']::FLOAT     AS usage_amount_in_pricing_units,
      flatten_export.value['usage']['unit']::VARCHAR                      AS usage_unit,
      flatten_export.value['usage_start_time']::TIMESTAMP                 AS usage_start_time,
      flatten_export.value['usage_end_time']::TIMESTAMP                   AS usage_end_time,
      source.uploaded_at                                                  AS uploaded_at,
      {{ dbt_utils.surrogate_key([
          'billing_account_id',
          'cost',
          'cost_type',
          'credits',
          'currency',
          'currency_conversion_rate',
          'export_time',
          'invoice_month',
          'labels',
          'resource_country',
          'resource_location',
          'resource_region',
          'resource_zone',
          'folder_id',
          'project_id',
          'project_labels',
          'project_name',
          'service_id',
          'service_description',
          'sku_id',
          'sku_description',
          'system_labels',
          'pricing_unit',
          'usage_amount',
          'usage_amount_in_pricing_units',
          'usage_unit',
          'usage_start_time',
          'usage_end_time'] ) }}                                          AS primary_key

  FROM source,
  TABLE(FLATTEN(source.jsontext)) flatten_export

), grouped AS (
    SELECT
      primary_key,
      billing_account_id,
      cost_type,
      credits,
      currency,
      currency_conversion_rate,
      export_time,
      invoice_month,
      labels,
      resource_country,
      resource_location,
      resource_region,
      resource_zone,
      folder_id,
      project_id,
      project_labels,
      project_name,
      service_id,
      service_description,
      sku_id,
      sku_description,
      system_labels,
      pricing_unit,
      usage_unit,
      usage_start_time,
      usage_end_time,
      -- rows can have identical primary keys, but differerent uploaded_at times
      -- so this allows these to be grouped together still
      MAX(uploaded_at)                                                  AS uploaded_at,
      SUM(cost)                                                         AS cost,
      SUM(usage_amount)                                                 AS usage_amount,
      SUM(usage_amount_in_pricing_units)                                AS usage_amount_in_pricing_units
      FROM flattened
      {{ dbt_utils.group_by(n=26) }}
      --UALIFY ROW_NUMBER() OVER (PARTITION BY primary_key ORDER BY uploaded_at DESC) = 1

)

SELECT * FROM grouped
