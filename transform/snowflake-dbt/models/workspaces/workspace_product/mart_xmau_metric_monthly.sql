{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ('dim_date','dim_date'),
    ('fct_usage_event', 'fct_usage_event'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ])
}}

, fact_raw AS  (

    SELECT
        event_id,
        CAST(event_created_at AS DATE)                                                                  AS event_date,
        event_created_at,
        dim_user_id,
        fct_usage_event.event_name,
        data_source                                                                                     AS data_source,
        plan_id_at_event_date,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        dim_namespace_id,
        is_umau,
        gmau                                                                                            AS is_gmau,
        smau                                                                                            AS is_smau,
        fct_usage_event.section_name,
        fct_usage_event.stage_name,
        fct_usage_event.group_name,
        DATE_TRUNC('MONTH', event_date)                                                                 AS reporting_month,
        QUARTER(event_date)                                                                             AS reporting_quarter,
        YEAR(event_date)                                                                                AS reporting_year
    FROM fct_usage_event
    LEFT JOIN xmau_metrics
      ON fct_usage_event.event_name = xmau_metrics.events_to_include
    WHERE is_umau = TRUE OR is_gmau = TRUE OR is_smau = TRUE

), fact_with_date_range AS (

    SELECT
        fact.event_id,
        fact.event_date,
        fact.event_created_at,
        dim_date.last_day_of_month                                                                      AS last_day_of_month,
        dim_date.last_day_of_quarter                                                                    AS last_day_of_quarter,
        dim_date.last_day_of_fiscal_year                                                                AS last_day_of_fiscal_year,
        fact.dim_user_id,
        fact.event_name,
        fact.data_source,
        fact.plan_was_paid_at_event_date,
        fact.dim_namespace_id,
        fact.is_umau,
        fact.is_gmau,
        fact.is_smau,
        fact.section_name,
        fact.stage_name,
        fact.group_name,
        fact.reporting_month,
        fact.reporting_quarter,
        fact.reporting_year
    FROM fact_raw as fact
    LEFT JOIN dim_date
        ON fact.event_date = dim_date.DATE_ACTUAL
    WHERE fact.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

), paid_flag_by_month AS (

      SELECT
          dim_namespace_id,
          reporting_month,
          plan_was_paid_at_event_date
      FROM fact_with_date_range
          QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id, reporting_month
                                     ORDER BY event_created_at DESC) = 1

 ), fact_w_paid_deduped AS (

     SELECT
         fact_with_date_range.event_id,
         fact_with_date_range.event_date,
         fact_with_date_range.last_day_of_month,
         fact_with_date_range.last_day_of_quarter,
         fact_with_date_range.last_day_of_fiscal_year,
         fact_with_date_range.dim_user_id,
         fact_with_date_range.event_name,
         fact_with_date_range.data_source,
         fact_with_date_range.dim_namespace_id,
         fact_with_date_range.is_umau,
         fact_with_date_range.is_gmau,
         fact_with_date_range.is_smau,
         fact_with_date_range.section_name,
         fact_with_date_range.stage_name,
         fact_with_date_range.group_name,
         fact_with_date_range.reporting_month,
         fact_with_date_range.reporting_quarter,
         fact_with_date_range.reporting_year,
         paid_flag_by_month.plan_was_paid_at_event_date
     FROM fact_with_date_range
         LEFT JOIN paid_flag_by_month
             ON fact_with_date_range.dim_namespace_id = paid_flag_by_month.dim_namespace_id AND fact_with_date_range.reporting_month = paid_flag_by_month.reporting_month

), total_results AS (

   SELECT
       reporting_month,
       is_umau,
       is_gmau,
       is_smau,
       section_name,
       stage_name,
       group_name,
       'total'                                                                                         AS user_group,
       ARRAY_AGG(distinct event_name) WITHIN GROUP (ORDER BY event_name)                               AS event_name_array,
       COUNT(*)                                                                                        AS event_count,
       COUNT(DISTINCT(dim_namespace_id))                                                               AS namespace_count,
       COUNT(DISTINCT(dim_user_id))                                                                    AS user_count
   FROM fact_w_paid_deduped
      {{ dbt_utils.group_by(n=8) }}
   ORDER BY reporting_month DESC

), free_results AS (

   SELECT
       reporting_month,
       is_umau,
       is_gmau,
       is_smau,
       section_name,
       stage_name,
       group_name,
       'free'                                                                                         AS user_group,
       ARRAY_AGG(distinct event_name) WITHIN GROUP (ORDER BY event_name)                              AS event_name_array,
       COUNT(*)                                                                                       AS event_count,
       COUNT(DISTINCT(dim_namespace_id))                                                              AS namespace_count,
       COUNT(DISTINCT(dim_user_id))                                                                   AS user_count
   FROM fact_w_paid_deduped
   WHERE plan_was_paid_at_event_date = FALSE
       {{ dbt_utils.group_by(n=8) }}
   ORDER BY reporting_month DESC

), paid_results AS (

   SELECT
       reporting_month,
       is_umau,
       is_gmau,
       is_smau,
       section_name,
       stage_name,
       group_name,
       'paid'                                                                                          AS user_group,
       ARRAY_AGG(distinct event_name) WITHIN GROUP (ORDER BY event_name)                               AS event_name_array,
       COUNT(*)                                                                                        AS event_count,
       COUNT(DISTINCT(dim_namespace_id))                                                               AS namespace_count,
       COUNT(DISTINCT(dim_user_id))                                                                    AS user_count
   FROM fact_w_paid_deduped
   WHERE plan_was_paid_at_event_date = TRUE
       {{ dbt_utils.group_by(n=8) }}
   ORDER BY reporting_month DESC

), results_wo_pk AS (

  SELECT * FROM total_results
    UNION ALL
  SELECT * FROM free_results
    UNION ALL
  SELECT * FROM paid_results

), results AS (

  SELECT
    {{ dbt_utils.surrogate_key(['reporting_month', 'user_group', 'section_name', 'stage_name', 'group_name']) }}                  AS mart_xmau_metric_monthly_id,
    *
  FROM results_wo_pk

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper_acp",
    updated_by="@icooper_acp",
    created_date="2022-02-23",
    updated_date="2022-03-03"
) }}
