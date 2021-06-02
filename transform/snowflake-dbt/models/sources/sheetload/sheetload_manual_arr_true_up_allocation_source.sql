WITH source AS (

    SELECT
      account_id::VARCHAR             AS account_id,
      crm_account_id::VARCHAR         AS crm_account_id,
      rate_plan_charge_id::VARCHAR    AS rate_plan_charge_id,
      dim_subscription_id::VARCHAR    AS dim_subcription_id,
      subscription_name::VARCHAR      AS subscription_name,
      subscription_status::VARCHAR    AS subscription_status,
      dim_product_detail_id::VARCHAR  AS dim_product_detail_id,
      mrr::NUMBER                     AS mrr,
      delta_tcv::NUMBER               AS delta_tcv,
      unit_of_measure::NUMBER         AS unit_of_measure,
      quantity::NUMBER                AS quantity,
      effective_start_date::DATE      AS effective_start_date,
      effective_end_date::DATE        AS effective_end_date
    FROM {{ source('sheetload','manual_arr_true_up_allocation') }}

)

SELECT *
FROM source
