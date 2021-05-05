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
      account_owner_team,
      tsp_territory,
      tsp_region,
      tsp_sub_region,
      tsp_area,
      gtm_strategy
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
    sfdc_account.account_id                       AS dim_crm_account_id,
    sfdc_account.account_name                     AS crm_account_name,
    sfdc_account.billing_country                  AS crm_account_billing_country,
    sfdc_account.account_type                     AS crm_account_type,
    sfdc_account.df_industry                      AS crm_account_industry,
    sfdc_account.account_owner                    AS crm_account_owner,
    sfdc_account.account_owner_team               AS crm_account_owner_team,
    sfdc_account.tsp_territory                    AS crm_account_sales_territory,
    sfdc_account.tsp_region                       AS crm_account_tsp_region,
    sfdc_account.tsp_sub_region                   AS crm_account_tsp_sub_region,
    sfdc_account.tsp_area                         AS crm_account_tsp_area,
    sfdc_account.gtm_strategy                     AS crm_account_gtm_strategy,
    CASE
      WHEN LOWER(sfdc_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
      ELSE 'Non - Focus Account'
    END                                           AS crm_account_focus_account,
    sfdc_account.health_score,
    sfdc_account.health_number,
    sfdc_account.health_score_color,
    ultimate_parent_account.account_id            AS dim_parent_crm_account_id,
    ultimate_parent_account.account_name          AS parent_crm_account_name,
    {{ sales_segment_cleaning('sfdc_account.ultimate_parent_sales_segment') }}
                                                  AS parent_crm_account_sales_segment,
    ultimate_parent_account.billing_country       AS parent_crm_account_billing_country,
    ultimate_parent_account.df_industry           AS parent_crm_account_industry,
    ultimate_parent_account.account_owner_team    AS parent_crm_account_owner_team,
    ultimate_parent_account.tsp_territory         AS parent_crm_account_sales_territory,
    ultimate_parent_account.tsp_region            AS parent_crm_account_tsp_region,
    ultimate_parent_account.tsp_sub_region        AS parent_crm_account_tsp_sub_region,
    ultimate_parent_account.tsp_area              AS parent_crm_account_tsp_area,
    ultimate_parent_account.gtm_strategy          AS parent_crm_account_gtm_strategy,
    CASE
      WHEN LOWER(ultimate_parent_account.gtm_strategy) IN ('account centric', 'account based - net new', 'account based - expand') THEN 'Focus Account'
      ELSE 'Non - Focus Account'
    END                                           AS parent_crm_account_focus_account,
    sfdc_account.record_type_id                   AS record_type_id,
    sfdc_account.federal_account                  AS federal_account,
    sfdc_account.gitlab_com_user,
    sfdc_account.tsp_account_employees,
    sfdc_account.tsp_max_family_employees,
    sfdc_users.name                               AS technical_account_manager,
    sfdc_account.is_deleted                       AS is_deleted,
    map_merged_crm_account.dim_crm_account_id    AS merged_to_account_id,
    IFF(sfdc_record_type.record_type_label != 'Channel'
        AND sfdc_account.account_type NOT IN ('Unofficial Reseller','Authorized Reseller','Prospective Partner','Partner','Former Reseller','Reseller','Prospective Reseller'),
        FALSE, TRUE)                              AS is_reseller
  FROM sfdc_account
  LEFT JOIN map_merged_crm_account
    ON sfdc_account.account_id = map_merged_crm_account.sfdc_account_id
  LEFT JOIN ultimate_parent_account
    ON sfdc_account.ultimate_parent_account_id = ultimate_parent_account.account_id
  LEFT OUTER JOIN sfdc_users
    ON sfdc_account.technical_account_manager_id = sfdc_users.user_id
  LEFT JOIN sfdc_record_type
    ON sfdc_account.record_type_id = sfdc_record_type.record_type_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@paul_armstrong",
    created_date="2020-06-01",
    updated_date="2021-04-26"
) }}
