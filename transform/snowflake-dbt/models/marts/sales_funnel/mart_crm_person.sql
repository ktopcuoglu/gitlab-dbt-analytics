{{config({
    "schema": "common_mart_marketing"
  })
}}

WITH dim_crm_person AS (

    SELECT *
    FROM {{ ref('dim_crm_person') }}

), dim_marketing_channel AS (

    SELECT *
    FROM {{ ref('dim_marketing_channel') }}

), dim_sales_segment AS (

    SELECT *
    FROM {{ ref('dim_sales_segment') }}

), fct_crm_person AS (

    SELECT *
    FROM {{ ref('fct_crm_person') }}

), final AS (

    SELECT
      fct_crm_person.dim_crm_person_id,
      fct_crm_person.mql_date_first_id,
      fct_crm_person.mql_date_first,
      DATE_TRUNC(month, fct_crm_person.mql_date_first)           AS mql_month_first,
      fct_crm_person.mql_date_latest,
      DATE_TRUNC(month, fct_crm_person.mql_date_latest)          AS mql_month_latest,
      fct_crm_person.created_date,
      DATE_TRUNC(month, fct_crm_person.created_date)             AS created_month,
      fct_crm_person.inquiry_date,
      DATE_TRUNC(month, fct_crm_person.inquiry_date)             AS inquiry_month,
      fct_crm_person.accepted_date,
      DATE_TRUNC(month, fct_crm_person.accepted_date)            AS accepted_month,
      fct_crm_person.qualifying_date,
      DATE_TRUNC(month, fct_crm_person.qualifying_date)          AS qualifying_month,
      fct_crm_person.qualified_date,
      DATE_TRUNC(month, fct_crm_person.qualified_date)           AS qualified_month,
      fct_crm_person.converted_date,
      DATE_TRUNC(month, fct_crm_person.converted_date)           AS converted_month,
      dim_crm_person.email_hash,
      dim_crm_person.lead_source,
      dim_crm_person.source_buckets,
      dim_marketing_channel.marketing_channel_name,
      CASE
        WHEN LOWER(dim_sales_segment.sales_segment_name) LIKE '%unknown%' THEN 'SMB'
        WHEN LOWER(dim_sales_segment.sales_segment_name) LIKE '%mid%' THEN 'Mid-Market'
        ELSE dim_sales_segment.sales_segment_name
      END                                                        AS sales_segment_name,
      fct_crm_person.is_mql
    FROM fct_crm_person
    LEFT JOIN dim_crm_person
      ON fct_crm_person.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN dim_sales_segment
      ON fct_crm_person.dim_sales_segment_id = dim_sales_segment.dim_sales_segment_id
    LEFT JOIN dim_marketing_channel
      ON fct_crm_person.dim_marketing_channel_id = dim_marketing_channel.dim_marketing_channel_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2020-12-07",
    updated_date="2021-02-08",
  ) }}
