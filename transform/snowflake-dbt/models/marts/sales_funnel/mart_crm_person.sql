{{config({
    "schema": "common_mart_marketing"
  })
}}

WITH dim_marketing_channel AS (

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
      dim_marketing_channel.marketing_channel_name,
      CASE
        WHEN LOWER(dim_sales_segment.sales_segment_name) LIKE '%unknown%' THEN 'SMB'
        WHEN LOWER(dim_sales_segment.sales_segment_name) LIKE '%mid%' THEN 'Mid-Market'
        ELSE dim_sales_segment.sales_segment_name
      END                                      AS sales_segment_name,
      fct_crm_person.is_mql
    FROM fct_crm_person
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
    updated_date="2020-12-07",
  ) }}
