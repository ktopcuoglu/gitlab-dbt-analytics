{{ config({
        "materialized": "view",
    })
}}

WITH sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE NOT is_deleted

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
      tsp_area
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

), sfdc_account_with_ultimate_parent AS (

    SELECT
      sfdc_account.account_id                                                               AS crm_account_id,
      ultimate_parent_account.account_id                                                    AS ultimate_parent_account_id,
      {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}            AS ultimate_parent_sales_segment,
      ultimate_parent_account.billing_country                                               AS ultimate_parent_billing_country,
      ultimate_parent_account.df_industry                                                   AS ultimate_parent_df_industry,
      ultimate_parent_account.tsp_territory                                                 AS ultimate_parent_tsp_territory,
      ultimate_parent_account.tsp_region                                                    AS ultimate_parent_tsp_region,
      ultimate_parent_account.tsp_sub_region                                                AS ultimate_parent_tsp_sub_region,
      ultimate_parent_account.tsp_area                                                      AS ultimate_parent_tsp_area,
      {{ sales_segment_cleaning("sfdc_account.ultimate_parent_sales_segment") }}            AS sales_segment,
      sfdc_account.billing_country,
      sfdc_account.df_industry,
      sfdc_account.tsp_territory,
      sfdc_account.tsp_region,
      sfdc_account.tsp_sub_region,
      sfdc_account.tsp_area
    FROM sfdc_account
    LEFT JOIN ultimate_parent_account
      ON sfdc_account.ultimate_parent_account_id = ultimate_parent_account.account_id

), sfdc_account_final AS (

    SELECT
      crm_account_id                                                                                        AS account_dim_crm_account_id,
      ultimate_parent_account_id                                                                            AS parent_dim_crm_account_id,
      TRIM(SPLIT_PART(tsp_region, '-', 1))                                                                  AS account_tsp_region_clean,
      TRIM(SPLIT_PART(ultimate_parent_tsp_region, '-', 1))                                                  AS parent_tsp_region_clean,
      TRIM(SPLIT_PART(tsp_sub_region, '-', 1))                                                              AS account_tsp_sub_region_clean,
      TRIM(SPLIT_PART(ultimate_parent_tsp_sub_region, '-', 1))                                              AS parent_tsp_sub_region_clean,
      TRIM(SPLIT_PART(REPLACE(tsp_area,'Mid - Atlantic', 'Mid Atlantic'), '-', 1))                          AS account_tsp_area_clean,
      TRIM(SPLIT_PART(REPLACE(ultimate_parent_tsp_area,'Mid - Atlantic', 'Mid Atlantic'), '-', 1))          AS parent_tsp_area_clean,
      TRIM(tsp_territory)                                                                                   AS account_tsp_territory_clean,
      TRIM(ultimate_parent_tsp_territory)                                                                   AS parent_tsp_territory_clean,
      TRIM(SPLIT_PART(df_industry, '-', 1))                                                                 AS account_df_industry_clean,
      TRIM(SPLIT_PART(ultimate_parent_df_industry, '-', 1))                                                 AS parent_df_industry_clean,
      TRIM(SPLIT_PART(sales_segment, '-', 1))                                                               AS account_sales_segment_clean,
      TRIM(SPLIT_PART(ultimate_parent_sales_segment, '-', 1))                                               AS parent_sales_segment_clean,
      TRIM(SPLIT_PART(billing_country, '-', 1))                                                             AS account_billing_country_clean,
      TRIM(SPLIT_PART(ultimate_parent_billing_country, '-', 1))                                             AS parent_billing_country_clean,
      MAX(account_tsp_region_clean) OVER (PARTITION BY UPPER(TRIM(account_tsp_region_clean)))               AS account_geo_region_name_source,
      MAX(parent_tsp_region_clean) OVER (PARTITION BY UPPER(TRIM(parent_tsp_region_clean)))                 AS parent_geo_region_name_source,
      MAX(account_tsp_sub_region_clean) OVER (PARTITION BY UPPER(TRIM(account_tsp_sub_region_clean)))       AS account_geo_sub_region_name_source,
      MAX(parent_tsp_sub_region_clean) OVER (PARTITION BY UPPER(TRIM(parent_tsp_sub_region_clean)))         AS parent_geo_sub_region_name_source,
      MAX(account_tsp_area_clean) OVER (PARTITION BY UPPER(TRIM(account_tsp_area_clean)))                   AS account_geo_area_name_source,
      MAX(parent_tsp_area_clean) OVER (PARTITION BY UPPER(TRIM(parent_tsp_area_clean)))                     AS parent_geo_area_name_source,
      MAX(account_tsp_territory_clean) OVER (PARTITION BY UPPER(TRIM(account_tsp_territory_clean)))         AS account_sales_territory_name_source,
      MAX(parent_tsp_territory_clean) OVER (PARTITION BY UPPER(TRIM(parent_tsp_territory_clean)))           AS parent_sales_territory_name_source,
      MAX(account_df_industry_clean) OVER (PARTITION BY UPPER(TRIM(account_df_industry_clean)))             AS account_industry_name_source,
      MAX(parent_df_industry_clean) OVER (PARTITION BY UPPER(TRIM(parent_df_industry_clean)))               AS parent_industry_name_source,
      MAX(account_sales_segment_clean) OVER (PARTITION BY UPPER(TRIM(account_sales_segment_clean)))         AS account_sales_segment_name_source,
      MAX(parent_sales_segment_clean) OVER (PARTITION BY UPPER(TRIM(parent_sales_segment_clean)))           AS parent_sales_segment_name_source,
      MAX(account_billing_country_clean) OVER (PARTITION BY UPPER(TRIM(account_billing_country_clean)))     AS account_location_country_name_source,
      MAX(parent_billing_country_clean) OVER (PARTITION BY UPPER(TRIM(parent_billing_country_clean)))       AS parent_location_country_name_source

    FROM sfdc_account_with_ultimate_parent

)

{{ dbt_audit(
    cte_ref="sfdc_account_final",
    created_by="@paul_armstrong",
    updated_by="@mcooperDD",
    created_date="2020-10-30",
    updated_date="2021-01-28"
) }}
