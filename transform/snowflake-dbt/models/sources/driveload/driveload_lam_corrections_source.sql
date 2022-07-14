WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'lam_corrections') }}

), renamed AS (

    SELECT
      first_day_of_month::VARCHAR                   AS first_day_of_month,
      fiscal_quarter_name_fy::VARCHAR               AS fiscal_quarter_name_fy,
      arr_month::VARCHAR                            AS arr_month,
      dim_parent_crm_account_id::VARCHAR            AS dim_parent_crm_account_id,
      parent_crm_account_sales_segment::VARCHAR     AS parent_crm_account_sales_segment,
      arr::VARCHAR                                  AS arr,
      dev_count::VARCHAR                            AS dev_count,
      estimated_lam::VARCHAR                        AS estimated_lam,
      estimated_capped_lam::VARCHAR                 AS estimated_capped_lam
    FROM source

)

SELECT *
FROM renamed
