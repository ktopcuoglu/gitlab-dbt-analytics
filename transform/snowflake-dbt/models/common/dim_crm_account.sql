WITH base AS (

    SELECT *
    FROM {{ ref('prep_crm_account') }}

), final AS (

    SELECT
      dim_crm_account_id                 AS dim_crm_account_id,
      crm_account_name                   AS crm_account_name,
      crm_account_billing_country        AS crm_account_billing_country,
      crm_account_type                   AS crm_account_type,
      crm_account_industry               AS crm_account_industry,
      crm_account_owner                  AS crm_account_owner,
      crm_account_owner_team             AS crm_account_owner_team,
      crm_account_sales_territory        AS crm_account_sales_territory,
      crm_account_tsp_region             AS crm_account_tsp_region,
      crm_account_tsp_sub_region         AS crm_account_tsp_sub_region,
      crm_account_tsp_area               AS crm_account_tsp_area,
      crm_account_gtm_strategy           AS crm_account_gtm_strategy,
      crm_account_focus_account          AS crm_account_focus_account,
      dim_parent_crm_account_id          AS dim_parent_crm_account_id,
      parent_crm_account_name            AS parent_crm_account_name,
      parent_crm_account_sales_segment   AS parent_crm_account_sales_segment,
      parent_crm_account_billing_country AS parent_crm_account_billing_country,
      parent_crm_account_industry        AS parent_crm_account_industry,
      parent_crm_account_owner_team      AS parent_crm_account_owner_team,
      parent_crm_account_sales_territory AS parent_crm_account_sales_territory,
      parent_crm_account_tsp_region      AS parent_crm_account_tsp_region,
      parent_crm_account_tsp_sub_region  AS parent_crm_account_tsp_sub_region,
      parent_crm_account_tsp_area        AS parent_crm_account_tsp_area,
      parent_crm_account_gtm_strategy    AS parent_crm_account_gtm_strategy,
      parent_crm_account_focus_account   AS parent_crm_account_focus_account,
      record_type_id                     AS record_type_id,
      federal_account                    AS federal_account,
      is_jihu_account                    AS is_jihu_account,
      technical_account_manager          AS technical_account_manager,
      is_deleted                         AS is_deleted,
      merged_to_account_id               AS merged_to_account_id,
      is_reseller                        AS is_reseller
    FROM base
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@paul_armstrong",
    created_date="2020-06-01",
    updated_date="2021-06-02"
) }}
