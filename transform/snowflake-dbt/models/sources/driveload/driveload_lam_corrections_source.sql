WITH source AS (

    SELECT
      dim_parent_crm_account_id                  AS dim_parent_crm_account_id,
      dev_count                                  AS dev_count,
      estimated_capped_lam                       AS estimated_capped_lam,
      parent_crm_account_sales_segment           AS dim_parent_crm_account_sales_segment,
      MIN(first_day_of_month)                    AS valid_from,
      MAX(first_day_of_month)                    AS valid_to
    FROM {{ source('driveload', 'lam_corrections') }}
    {{ dbt_utils.group_by(n=4) }}
)

SELECT *
FROM source
