WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'financial_metrics_program_phase_1') }}

), renamed AS (

    SELECT
      arr_month::DATE                                     arr_month,
      fiscal_quarter_name_fy::TEXT                        fiscal_quarter_name_fy,
      fiscal_year::NUMBER                                 fiscal_year,
      subscription_start_month::TIMESTAMP_TZ              subscription_start_month,
      subscription_end_month::TIMESTAMP_TZ                subscription_end_month,
      zuora_account_id::TEXT                              zuora_account_id,
      zuora_sold_to_country::TEXT                         zuora_sold_to_country,
      zuora_account_name::TEXT                            zuora_account_name,
      zuora_account_number::TEXT                          zuora_account_number,
      dim_crm_account_id::TEXT                            dim_crm_account_id,
      dim_parent_crm_account_id::TEXT                     dim_parent_crm_account_id,
      parent_crm_account_name::TEXT                       parent_crm_account_name,
      parent_crm_account_billing_country::TEXT            parent_crm_account_billing_country,
      parent_crm_account_sales_segment::TEXT              parent_crm_account_sales_segment,
      parent_crm_account_industry::TEXT                   parent_crm_account_industry,
      parent_crm_account_owner_team::TEXT                 parent_crm_account_owner_team,
      parent_crm_account_sales_territory::TEXT            parent_crm_account_sales_territory,
      subscription_name::TEXT                             subscription_name,
      subscription_status::TEXT                           subscription_status,
      subscription_sales_type::TEXT                       subscription_sales_type,
      product_category::TEXT                              product_category,
      delivery::TEXT                                      delivery,
      service_type::TEXT                                  service_type,
      unit_of_measure::ARRAY                              unit_of_measure,
      mrr::FLOAT                                          mrr,
      arr::FLOAT                                          arr,
      quantity::FLOAT                                     quantity,
      parent_account_cohort_month::DATE                   parent_account_cohort_month,
      months_since_parent_account_cohort_start::NUMBER    months_since_parent_account_cohort_start,
      arr_band_calc::TEXT                                 arr_band_calc,
      product_name::TEXT                                  product_name
    FROM source

)

SELECT *
FROM renamed
