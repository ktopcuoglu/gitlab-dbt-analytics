WITH map_merged_crm_account AS (

    SELECT *
    FROM {{ ref('map_merged_crm_account') }}

), sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}

), ultimate_parent_account AS (

    SELECT
      account_id,
      account_name,
      billing_country,
      df_industry,
      industry,
      sub_industry,
      account_owner_team,
      tsp_territory,
      tsp_region,
      tsp_sub_region,
      tsp_area,
      gtm_strategy,
      tsp_account_employees,
      tsp_max_family_employees,
      created_date,
      zi_technologies,
      zoom_info_website,
      zoom_info_company_other_domains,
      zoom_info_dozisf_zi_id,
      zoom_info_parent_company_zi_id,
      zoom_info_parent_company_name,
      zoom_info_ultimate_parent_company_zi_id,
      zoom_info_ultimate_parent_company_name
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

), deleted_accounts AS (

    SELECT *
    FROM sfdc_account
    WHERE is_deleted = TRUE

), master_records AS (

    SELECT
      a.account_id,
      COALESCE(
      b.master_record_id, a.master_record_id) AS sfdc_master_record_id
    FROM deleted_accounts a
    LEFT JOIN deleted_accounts b
      ON a.master_record_id = b.account_id

), final AS (

  SELECT
    --crm account informtion
    sfdc_account.owner_id                               AS dim_crm_user_id,
    sfdc_account.account_id                             AS dim_crm_account_id,
    sfdc_account.account_name                           AS crm_account_name,
    sfdc_account.billing_country                        AS crm_account_billing_country,
    sfdc_account.account_type                           AS crm_account_type,
    sfdc_account.industry                               AS crm_account_industry,
    sfdc_account.sub_industry                           AS crm_account_sub_industry,
    sfdc_account.account_owner                          AS crm_account_owner,
    sfdc_account.account_owner_team                     AS crm_account_owner_team,
    sfdc_account.tsp_territory                          AS crm_account_sales_territory,
    sfdc_account.tsp_region                             AS crm_account_tsp_region,
    sfdc_account.tsp_sub_region                         AS crm_account_tsp_sub_region,
    sfdc_account.tsp_area                               AS crm_account_tsp_area,
    sfdc_account.gtm_strategy                           AS crm_account_gtm_strategy,
    CASE
      WHEN LOWER(sfdc_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
      ELSE 'Non - Focus Account'
    END                                                 AS crm_account_focus_account,
    sfdc_account.account_owner_user_segment             AS crm_account_owner_user_segment,
    sfdc_account.tsp_account_employees                  AS crm_account_tsp_account_employees,
    sfdc_account.tsp_max_family_employees               AS crm_account_tsp_max_family_employees,
    CASE
       WHEN sfdc_account.tsp_max_family_employees > 2000 THEN 'Employees > 2K'
       WHEN sfdc_account.tsp_max_family_employees <= 2000 AND sfdc_account.tsp_max_family_employees > 1500 THEN 'Employees > 1.5K'
       WHEN sfdc_account.tsp_max_family_employees <= 1500 AND sfdc_account.tsp_max_family_employees > 1000  THEN 'Employees > 1K'
       ELSE 'Employees < 1K'
    END                                                 AS crm_account_employee_count_band,
    sfdc_account.health_score,
    sfdc_account.health_number,
    sfdc_account.health_score_color,
    sfdc_account.partner_account_iban_number,
    CAST(sfdc_account.partners_signed_contract_date AS date)
                                                        AS partners_signed_contract_date,
    sfdc_account.record_type_id                         AS record_type_id,
    sfdc_account.federal_account                        AS federal_account,
    sfdc_account.is_jihu_account                        AS is_jihu_account,
    sfdc_account.carr_this_account,
    sfdc_account.carr_total,
    sfdc_account.potential_arr_lam,
    sfdc_account.fy22_new_logo_target_list,
    sfdc_account.is_first_order_available,
    sfdc_account.gitlab_com_user,
    sfdc_account.tsp_account_employees,
    sfdc_account.tsp_max_family_employees,
    account_owner.name                                                  AS account_owner,
    sfdc_users.name                                                     AS technical_account_manager,
    sfdc_account.is_deleted                                             AS is_deleted,
    map_merged_crm_account.dim_crm_account_id                           AS merged_to_account_id,
    IFF(sfdc_record_type.record_type_label = 'Partner'
        AND sfdc_account.partner_type IN ('Alliance', 'Channel')
        AND sfdc_account.partner_status = 'Authorized',
        TRUE, FALSE)                                        AS is_reseller,
    sfdc_account.created_date                               AS crm_account_created_date,
    sfdc_account.zi_technologies                            AS crm_account_zi_technologies,
    sfdc_account.technical_account_manager_date,
    sfdc_account.zoom_info_website                          AS crm_account_zoom_info_website,
    sfdc_account.zoom_info_company_other_domains            AS crm_account_zoom_info_company_other_domains,
    sfdc_account.zoom_info_dozisf_zi_id                     AS crm_account_zoom_info_dozisf_zi_id,
    sfdc_account.zoom_info_parent_company_zi_id             AS crm_account_zoom_info_parent_company_zi_id,
    sfdc_account.zoom_info_parent_company_name              AS crm_account_zoom_info_parent_company_name,
    sfdc_account.zoom_info_ultimate_parent_company_zi_id    AS crm_account_zoom_info_ultimate_parent_company_zi_id,
    sfdc_account.zoom_info_ultimate_parent_company_name     AS crm_account_zoom_info_ultimate_parent_company_name,

    ----ultimate parent crm account info
    ultimate_parent_account.account_id                  AS dim_parent_crm_account_id,
    ultimate_parent_account.account_name                AS parent_crm_account_name,
    {{ sales_segment_cleaning('sfdc_account.ultimate_parent_sales_segment') }}
                                                        AS parent_crm_account_sales_segment,
    ultimate_parent_account.billing_country             AS parent_crm_account_billing_country,
    ultimate_parent_account.industry                    AS parent_crm_account_industry,
    ultimate_parent_account.sub_industry                AS parent_crm_account_sub_industry,
    ultimate_parent_account.account_owner_team          AS parent_crm_account_owner_team,
    ultimate_parent_account.tsp_territory               AS parent_crm_account_sales_territory,
    ultimate_parent_account.tsp_region                  AS parent_crm_account_tsp_region,
    ultimate_parent_account.tsp_sub_region              AS parent_crm_account_tsp_sub_region,
    ultimate_parent_account.tsp_area                    AS parent_crm_account_tsp_area,
    ultimate_parent_account.gtm_strategy                AS parent_crm_account_gtm_strategy,
    CASE
      WHEN LOWER(ultimate_parent_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
      ELSE 'Non - Focus Account'
    END                                                 AS parent_crm_account_focus_account,
    ultimate_parent_account.tsp_account_employees       AS parent_crm_account_tsp_account_employees,
    ultimate_parent_account.tsp_max_family_employees    AS parent_crm_account_tsp_max_family_employees,
    CASE
       WHEN ultimate_parent_account.tsp_max_family_employees > 2000 THEN 'Employees > 2K'
       WHEN ultimate_parent_account.tsp_max_family_employees <= 2000 AND ultimate_parent_account.tsp_max_family_employees > 1500 THEN 'Employees > 1.5K'
       WHEN ultimate_parent_account.tsp_max_family_employees <= 1500 AND ultimate_parent_account.tsp_max_family_employees > 1000  THEN 'Employees > 1K'
       ELSE 'Employees < 1K'
    END                                                                AS parent_crm_account_employee_count_band,
    ultimate_parent_account.created_date                               AS parent_crm_account_created_date,
    ultimate_parent_account.zi_technologies                            AS parent_crm_account_zi_technologies,
    ultimate_parent_account.zoom_info_website                          AS parent_crm_account_zoom_info_website,
    ultimate_parent_account.zoom_info_company_other_domains            AS parent_crm_account_zoom_info_company_other_domains,
    ultimate_parent_account.zoom_info_dozisf_zi_id                     AS parent_crm_account_zoom_info_dozisf_zi_id,
    ultimate_parent_account.zoom_info_parent_company_zi_id             AS parent_crm_account_zoom_info_parent_company_zi_id,
    ultimate_parent_account.zoom_info_parent_company_name              AS parent_crm_account_zoom_info_parent_company_name,
    ultimate_parent_account.zoom_info_ultimate_parent_company_zi_id    AS parent_crm_account_zoom_info_ultimate_parent_company_zi_id,
    ultimate_parent_account.zoom_info_ultimate_parent_company_name     AS parent_crm_account_zoom_info_ultimate_parent_company_name
  FROM sfdc_account
  LEFT JOIN map_merged_crm_account
    ON sfdc_account.account_id = map_merged_crm_account.sfdc_account_id
  LEFT JOIN ultimate_parent_account
    ON sfdc_account.ultimate_parent_account_id = ultimate_parent_account.account_id
  LEFT OUTER JOIN sfdc_users
    ON sfdc_account.technical_account_manager_id = sfdc_users.user_id
  LEFT JOIN sfdc_users AS account_owner
    ON account_owner.user_id = sfdc_account.owner_id
  LEFT JOIN sfdc_record_type
    ON sfdc_account.record_type_id = sfdc_record_type.record_type_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@jpeguero",
    created_date="2020-06-01",
    updated_date="2021-12-02"
) }}
