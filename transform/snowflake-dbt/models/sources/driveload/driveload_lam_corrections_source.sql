WITH source AS (

    SELECT
      dim_parent_crm_account_id                  AS dim_parent_crm_account_id,
      parent_crm_account_sales_segment           AS dim_parent_crm_account_sales_segment,
      dev_count                                  AS dev_count,
      estimated_capped_lam                       AS estimated_capped_lam,
      valid_from                                 AS valid_from,
      valid_to_final_date                        AS valid_to
    FROM {{ source('driveload', 'lam_corrections') }}
)

SELECT *
FROM source
