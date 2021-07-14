WITH base AS (

    SELECT *
    FROM {{ ref('prep_crm_account') }}

), final AS (

    SELECT
      dim_crm_user_id                             AS dim_crm_user_id,
      dim_crm_account_id                          AS dim_crm_account_id,
      crm_account_name                            AS crm_account_name,
      crm_account_billing_country                 AS crm_account_billing_country,
      crm_account_type                            AS crm_account_type,
      crm_account_industry                        AS crm_account_industry,
      crm_account_owner                           AS crm_account_owner,
      crm_account_owner_team                      AS crm_account_owner_team,
      crm_account_sales_territory                 AS crm_account_sales_territory,
      crm_account_tsp_region                      AS crm_account_tsp_region,
      crm_account_tsp_sub_region                  AS crm_account_tsp_sub_region,
      crm_account_tsp_area                        AS crm_account_tsp_area,
      crm_account_gtm_strategy                    AS crm_account_gtm_strategy,
      crm_account_focus_account                   AS crm_account_focus_account,
      dim_parent_crm_account_id                   AS dim_parent_crm_account_id,
      parent_crm_account_name                     AS parent_crm_account_name,
      parent_crm_account_sales_segment            AS parent_crm_account_sales_segment,
      parent_crm_account_billing_country          AS parent_crm_account_billing_country,
      parent_crm_account_industry                 AS parent_crm_account_industry,
      parent_crm_account_owner_team               AS parent_crm_account_owner_team,
      parent_crm_account_sales_territory          AS parent_crm_account_sales_territory,
      parent_crm_account_tsp_region               AS parent_crm_account_tsp_region,
      parent_crm_account_tsp_sub_region           AS parent_crm_account_tsp_sub_region,
      parent_crm_account_tsp_area                 AS parent_crm_account_tsp_area,
      parent_crm_account_gtm_strategy             AS parent_crm_account_gtm_strategy,
      parent_crm_account_focus_account            AS parent_crm_account_focus_account,
      account_owner_user_segment                  AS account_owner_user_segment,
      CAST(partners_signed_contract_date AS date) AS partners_signed_contract_date,
      record_type_id                              AS record_type_id,
      federal_account                             AS federal_account,
      is_jihu_account                             AS is_jihu_account,
      potential_arr_lam                           AS potential_arr_lam,
      fy22_new_logo_target_list                   AS fy22_new_logo_target_list,
      is_first_order_available                    AS is_first_order_available,
      gitlab_com_user                             AS gitlab_com_user,
      tsp_account_employees                       AS tsp_account_employees,
      tsp_max_family_employees                    AS tsp_max_family_employees,
      technical_account_manager                   AS technical_account_manager,
      is_deleted                                  AS is_deleted,
      merged_to_account_id                        AS merged_to_account_id,
      is_reseller                                 AS is_reseller,
      health_score                                AS health_score,
      health_number                               AS health_number,
      health_score_color                          AS health_score_color,
      partner_account_iban_number                 AS partner_account_iban_number
    FROM base
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@jpeguero",
    created_date="2020-06-01",
    updated_date="2021-06-22"
) }}
