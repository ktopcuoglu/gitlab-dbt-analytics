{{config({
    "schema": "common_mart_sales"
  })
}}

{{ simple_cte([
    ('dim_crm_sales_hierarchy_live','dim_crm_sales_hierarchy_live'),
    ('dim_sales_qualified_source','dim_sales_qualified_source'),
    ('dim_order_type','dim_order_type'),
    ('fct_sales_funnel_target','fct_sales_funnel_target'),
    ('dim_date','dim_date')
]) }}

, monthly_targets AS (

    SELECT
      fct_sales_funnel_target.sales_funnel_target_id,
      fct_sales_funnel_target.first_day_of_month AS target_month,
      fct_sales_funnel_target.kpi_name,
      dim_crm_sales_hierarchy_live.sales_segment_name_live,
      dim_crm_sales_hierarchy_live.location_region_name_live,
      dim_crm_sales_hierarchy_live.sales_region_name_live,
      dim_crm_sales_hierarchy_live.sales_area_name_live,
      dim_crm_sales_hierarchy_live.segment_region_live_grouped,
      dim_order_type.order_type_name,
      dim_order_type.order_type_grouped,
      dim_sales_qualified_source.sales_qualified_source_name,
      fct_sales_funnel_target.allocated_target
    FROM fct_sales_funnel_target
    LEFT JOIN dim_sales_qualified_source
      ON fct_sales_funnel_target.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_order_type
      ON fct_sales_funnel_target.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_sales_hierarchy_live
      ON fct_sales_funnel_target.dim_crm_sales_hierarchy_sales_segment_live_id = dim_crm_sales_hierarchy_live.dim_crm_sales_hierarchy_sales_segment_live_id
      AND fct_sales_funnel_target.dim_crm_sales_hierarchy_location_region_live_id = dim_crm_sales_hierarchy_live.dim_crm_sales_hierarchy_location_region_live_id
      AND fct_sales_funnel_target.dim_crm_sales_hierarchy_sales_region_live_id = dim_crm_sales_hierarchy_live.dim_crm_sales_hierarchy_sales_region_live_id
      AND fct_sales_funnel_target.dim_crm_sales_hierarchy_sales_area_live_id = dim_crm_sales_hierarchy_live.dim_crm_sales_hierarchy_sales_area_live_id

), monthly_targets_daily AS (

    SELECT
      date_day,
      monthly_targets.*,
      DATEDIFF('day', first_day_of_month, last_day_of_month) + 1  AS days_of_month,
      fiscal_quarter_name,
      fiscal_year,
      allocated_target / days_of_month                            AS daily_allocated_target
    FROM monthly_targets
    INNER JOIN dim_date
      ON monthly_targets.target_month = dim_date.first_day_of_month

), qtd_mtd_target AS (

    SELECT
      {{ dbt_utils.surrogate_key(['date_day', 'kpi_name', 'sales_segment_name_live', 'location_region_name_live', 'sales_region_name_live',
        'sales_area_name_live', 'order_type_name', 'sales_qualified_source_name']) }}                                                                   AS primary_key,
      date_day                                                                                                                                      AS target_date,
      DATEADD('day', 1, target_date)                                                                                                                AS report_target_date,
      target_month,
      fiscal_quarter_name,
      fiscal_year,
      kpi_name,
      sales_segment_name_live,
      location_region_name_live,
      sales_region_name_live,
      sales_area_name_live,
      segment_region_live_grouped,
      order_type_name,
      order_type_grouped,
      sales_qualified_source_name,
      allocated_target                                                                                                                              AS monthly_allocated_target,
      daily_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, sales_segment_name_live, location_region_name_live, sales_region_name_live,
                             sales_area_name_live, order_type_name, sales_qualified_source_name, target_month ORDER BY date_day)                        AS mtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, sales_segment_name_live, location_region_name_live, sales_region_name_live,
                             sales_area_name_live, order_type_name, sales_qualified_source_name, fiscal_quarter_name ORDER BY date_day)                 AS qtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, sales_segment_name_live, location_region_name_live, sales_region_name_live,
                             sales_area_name_live, order_type_name, sales_qualified_source_name, fiscal_year ORDER BY date_day)                         AS ytd_allocated_target

    FROM monthly_targets_daily

)

{{ dbt_audit(
    cte_ref="qtd_mtd_target",
    created_by="@jpeguero",
    updated_by="@mcooperDD",
    created_date="2021-02-18",
    updated_date="2021-02-26",
  ) }}
