{%- set metrics = 
    ["MQLs / A",
    "MQLs / T",
    "MQLs / Ttmp",
    "Trials / A",
    "Trials / T",
    "Trials / Ttmp",
    "First Order ARR / A",
    "First Order ARR / T",
    "First Order ARR / Ttmp",
    "New Logos / A",
    "New Logos / T",
    "New Logos / Ttmp",
    "MQLs_RATIO / A",
    "MQLs_RATIO / T",
    "MQLs_RATIO / Ttmp",
    "SAOs / A",
    "SAOs / T",
    "SAOs / Ttmp",
    "Won Opps / A",
    "Won Opps / T",
    "Won Opps / Ttmp",
    "Total Opps / A",
    "Total Opps / T",
    "Total Opps / Ttmp"]
  -%}

{% set select_columns = ["segment_region_grouped"] %}
{% set num_of_cols_to_group = 2 %}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('rpt_crm_opportunity_closed_period', 'rpt_crm_opportunity_closed_period'),
    ('rpt_crm_opportunity_accepted_period', 'rpt_crm_opportunity_accepted_period'),
    ('rpt_crm_person_mql', 'rpt_crm_person_mql'),
    ('rpt_sales_funnel_target', 'rpt_sales_funnel_target'),
    ('rpt_sales_funnel_target_daily', 'rpt_sales_funnel_target_daily')
]) }}

, crm_person AS (

    SELECT
      {{ dbt_utils.star(from=ref('rpt_crm_person_mql'), except=['SALES_SEGMENT_GROUPED']) }},
      {{ null_or_missing('sales_segment_name', 'sales_segment') }},
      {{ null_or_missing('sales_segment_region_mapped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_crm_person_mql
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'
  
), crm_opportunity_closed_period AS (

    SELECT
      rpt_crm_opportunity_closed_period.*,
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped', 'sales_segment') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_region_stamped_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_opp_owner_sales_segment_stamped_grouped', 'sales_segment_grouped') }}
    FROM rpt_crm_opportunity_closed_period
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'

), crm_opportunity_accepted_period AS (

    SELECT 
      rpt_crm_opportunity_accepted_period.*,
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_crm_opportunity_accepted_period
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'

), sales_funnel_target AS (

    SELECT 
      {{ dbt_utils.star(from=ref('rpt_sales_funnel_target'), except=['ORDER_TYPE_GROUPED']) }},
      {{ null_or_missing('order_type_grouped', 'order_type_grouped') }},
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_sales_funnel_target

), sales_funnel_target_daily AS (

    SELECT 
      {{ dbt_utils.star(from=ref('rpt_sales_funnel_target_daily'), except=['ORDER_TYPE_GROUPED']) }},
      {{ null_or_missing('order_type_grouped', 'order_type_grouped') }},
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      {{ null_or_missing('sales_qualified_source_name', 'sales_qualified_source') }},
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_sales_funnel_target_daily
    WHERE order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'

), sales_funnel_target_mql_trial AS (

    SELECT 
      {{ dbt_utils.star(from=ref('rpt_sales_funnel_target'), except=['ORDER_TYPE_GROUPED']) }},
      {{ null_or_missing('order_type_grouped', 'order_type_grouped') }},
      {{ null_or_missing('crm_user_sales_segment', 'sales_segment') }},
      {{ null_or_missing('crm_user_sales_segment_region_grouped', 'segment_region_grouped') }},
      'Missing sales_qualified_source' AS sales_qualified_source,
      {{ null_or_missing('crm_user_sales_segment_grouped', 'sales_segment_grouped') }}
    FROM rpt_sales_funnel_target
    WHERE kpi_name IN ('MQL', 'Trials')
      AND order_type_grouped != '3) Consumption / PS / Other'
      AND order_type_grouped NOT LIKE '%Missing%'

), current_fiscal_quarter AS (
  
    SELECT DISTINCT fiscal_quarter_name_fy AS current_fiscal_quarter
    FROM dim_date
    WHERE date_actual = CURRENT_DATE

), factor_to_date AS (
  
    SELECT DISTINCT
      'date_range_quarter' AS _type,
      fiscal_quarter_name_fy::VARCHAR AS _date,
      IFF(fiscal_quarter_name_fy < current_fiscal_quarter.current_fiscal_quarter, TRUE, FALSE) AS is_selected_quarter_lower_than_current_quarter,
      last_day_of_fiscal_quarter
    FROM dim_date
    LEFT JOIN current_fiscal_quarter
    -- WHERE [fiscal_quarter_name_fy=bc_fiscal_quarter]
    WHERE fiscal_quarter_name_fy='FY22-Q1'

), base_list AS (
  
    SELECT
      fiscal_quarter_name_fy,
      order_type_grouped,
      sales_segment,
      segment_region_grouped,
      sales_qualified_source,
      sales_segment_grouped
    FROM crm_opportunity_closed_period
    
    UNION
    
    --SAOs
    SELECT
      fiscal_quarter_name_fy,
      order_type_grouped,
      sales_segment,
      segment_region_grouped,
      sales_qualified_source,
      sales_segment_grouped
    FROM crm_opportunity_accepted_period
    
    UNION
    
    SELECT
      fiscal_quarter_name_fy,
      order_type_grouped,
      sales_segment,
      segment_region_grouped,
      sales_qualified_source,
      sales_segment_grouped
    FROM crm_person
    
    UNION
    
    -- Targets
    SELECT
      fiscal_quarter_name_fy,
      order_type_grouped,
      sales_segment,
      segment_region_grouped,
      sales_qualified_source,
      sales_segment_grouped
    FROM sales_funnel_target
    
    UNION
    
    -- Targets MQL
    SELECT 
      fiscal_quarter_name_fy,
      order_type_grouped,
      sales_segment,
      segment_region_grouped,
      sales_qualified_source,
      sales_segment_grouped
    FROM sales_funnel_target_mql_trial
    WHERE kpi_name IN ('MQL', 'Trials')
  
), new_logos_actual AS (

    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      COUNT(DISTINCT dim_crm_opportunity_id)               AS actual_new_logos
    FROM crm_opportunity_closed_period
    WHERE is_won = 'TRUE'
      AND is_closed = 'TRUE'
      AND is_edu_oss = 0
      -- AND IFF([new_logos] = FALSE, TRUE, order_type = '1. New - First Order')
      AND IFF(FALSE = FALSE, TRUE, order_type = '1. New - First Order')
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}

), mql_count AS (  
  
    SELECT 
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      COUNT(DISTINCT email_hash)             AS mqls
    FROM crm_person
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
  
), sao_count AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      COUNT(*)                AS saos
    FROM crm_opportunity_accepted_period
    WHERE is_sao = TRUE
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
  
), first_oder_arr_closed_won AS (

    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      SUM(net_arr)            AS first_oder_arr_closed_won
    FROM crm_opportunity_closed_period
    WHERE is_won = 'TRUE'
      AND is_closed = 'TRUE'
      AND is_edu_oss = 0
      --AND order_type = '1. New - First Order'
      AND IFF(FALSE = FALSE, TRUE, order_type = '1. New - First Order')
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
  
), win_rate AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      COUNT(CASE WHEN is_won THEN 1 END)                            AS won_opps,
      COUNT(is_won)                                                 AS total_opps
    FROM crm_opportunity_closed_period
    WHERE is_win_rate_calc = TRUE
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
   
), actual_trials AS (
  
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      COUNT(DISTINCT email_hash, lead_source) AS actual_trials
    FROM crm_person
    WHERE is_lead_source_trial = TRUE
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}

), targets AS (
 
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      SUM(IFF(kpi_name = 'Trials', qtd_allocated_target, 0)) AS  target_trials,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', qtd_allocated_target, 0)) AS  target_new_logos,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', qtd_allocated_target, 0)) AS  target_net_arr_closed,
      SUM(IFF(IFF(FALSE = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', qtd_allocated_target, 0)) AS  target_new_logos,
      SUM(IFF(IFF(FALSE = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', qtd_allocated_target, 0)) AS  target_net_arr_closed,
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', qtd_allocated_target, 0)) AS  target_sao,
      SUM(IFF(kpi_name = 'MQL', qtd_allocated_target, 0)) AS  target_mql,
      SUM(IFF(kpi_name = 'Deals', qtd_allocated_target, 0)) AS  target_won_opps,
      SUM(IFF(kpi_name = 'Total Closed', qtd_allocated_target, 0)) AS  target_total_opps
    
    FROM sales_funnel_target_daily
    LEFT JOIN factor_to_date
    WHERE IFF(factor_to_date.is_selected_quarter_lower_than_current_quarter, target_date = factor_to_date.last_day_of_fiscal_quarter, report_target_date = CURRENT_DATE)
      --AND [fiscal_quarter_name_fy=bc_fiscal_quarter]
      AND fiscal_quarter_name_fy='FY22-Q1'
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
  
), targets_full AS (
 
    SELECT
      fiscal_quarter_name_fy,
      {% for select_column in select_columns %}
      {{ select_column }},
      {% endfor %}
      SUM(IFF(kpi_name = 'Trials', allocated_target, 0)) AS  target_trials_full,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', allocated_target, 0)) AS  target_new_logos_full,
      --SUM(IFF(IFF([new_logos] = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', allocated_target, 0)) AS  target_net_arr_closed_full,
      SUM(IFF(IFF(FALSE = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Deals', allocated_target, 0)) AS  target_new_logos_full,
      SUM(IFF(IFF(FALSE = FALSE, TRUE, order_type_name = '1. New - First Order') AND kpi_name = 'Net ARR', allocated_target, 0)) AS  target_net_arr_closed_full,
      SUM(IFF(kpi_name = 'Stage 1 Opportunities', allocated_target, 0)) AS  target_sao_full,
      SUM(IFF(kpi_name = 'MQL', allocated_target, 0)) AS  target_mql_full,
      SUM(IFF(kpi_name = 'Deals', allocated_target, 0)) AS  target_won_opps_full,
      SUM(IFF(kpi_name = 'Total Closed', allocated_target, 0)) AS  target_total_opps_full
    
    FROM sales_funnel_target
    GROUP BY 1,
      {% for select_column in select_columns %}
        {{ select_column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
  
), agg AS (

  SELECT
    
    {% for select_column in select_columns %}
    base_list.{{select_column}},
    {% endfor %}

    {{num_of_cols_to_group}}                   AS num_of_cols_to_group,
    IFNULL(mqls, 0)                            AS "MQLs / A",
    NULLIF(target_mql_full, 0)                 AS "MQLs / T",
    NULLIF(target_mql, 0)                      AS "MQLs / Ttmp",
    IFNULL(actual_trials, 0)                   AS "Trials / A",
    NULLIF(target_trials_full, 0)              AS "Trials / T",
    NULLIF(target_trials, 0)                   AS "Trials / Ttmp",
    IFNULL(first_oder_arr_closed_won, 0)       AS "First Order ARR / A",
    NULLIF(target_net_arr_closed_full, 0)      AS "First Order ARR / T",
    NULLIF(target_net_arr_closed, 0)           AS "First Order ARR / Ttmp",
    IFNULL(actual_new_logos, 0)                AS "New Logos / A",
    NULLIF(target_new_logos_full, 0)           AS "New Logos / T",
    NULLIF(target_new_logos, 0)                AS "New Logos / Ttmp",
    IFNULL(mqls, 0)                            AS "MQLs_RATIO / A",
    NULLIF(target_mql_full, 0)                 AS "MQLs_RATIO / T",
    NULLIF(target_mql, 0)                      AS "MQLs_RATIO / Ttmp",
    IFNULL(saos, 0)                            AS "SAOs / A",
    NULLIF(target_sao_full, 0)                 AS "SAOs / T",
    NULLIF(target_sao, 0)                      AS "SAOs / Ttmp",
    IFNULL(won_opps, 0)                        AS "Won Opps / A",
    NULLIF(target_won_opps_full, 0)            AS "Won Opps / T",
    NULLIF(target_won_opps, 0)                 AS "Won Opps / Ttmp",
    IFNULL(total_opps, 0)                      AS "Total Opps / A",
    NULLIF(target_total_opps_full, 0)          AS "Total Opps / T",
    NULLIF(target_total_opps, 0)               AS "Total Opps / Ttmp"

  FROM base_list

  INNER JOIN factor_to_date
    ON base_list.fiscal_quarter_name_fy::VARCHAR = factor_to_date._date::VARCHAR

  LEFT JOIN new_logos_actual
    ON base_list.fiscal_quarter_name_fy = new_logos_actual.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = new_logos_actual.select_column
    {% endfor %}

  LEFT JOIN first_oder_arr_closed_won
    ON base_list.fiscal_quarter_name_fy = first_oder_arr_closed_won.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = first_oder_arr_closed_won.select_column
    {% endfor %}

  LEFT JOIN mql_count
    ON base_list.fiscal_quarter_name_fy = mql_count.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = mql_count.select_column
    {% endfor %}

  LEFT JOIN sao_count
    ON base_list.fiscal_quarter_name_fy = sao_count.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = sao_count.select_column
    {% endfor %}

  LEFT JOIN win_rate
    ON base_list.fiscal_quarter_name_fy = win_rate.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = win_rate.select_column
    {% endfor %}
  
  LEFT JOIN actual_trials
    ON base_list.fiscal_quarter_name_fy = actual_trials.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = actual_trials.select_column
    {% endfor %}
  
  LEFT JOIN targets
    ON base_list.fiscal_quarter_name_fy = targets.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = targets.select_column
    {% endfor %}
  LEFT JOIN targets_full
    ON base_list.fiscal_quarter_name_fy = targets_full.fiscal_quarter_name_fy
    {% for select_column in select_colummns %}
    AND base_list.select_column = targets_full.select_column
    {% endfor %}
  
  WHERE NOT (
    "First Order ARR / A" = 0 AND "First Order ARR / T" IS NULL
    AND "New Logos / A" = 0   AND "New Logos / T"       IS NULL
    AND "MQLs / A" = 0        AND "MQLs / T"            IS NULL
    AND "SAOs / A" = 0        AND "SAOs / T"            IS NULL
    AND "Won Opps / A" = 0    AND "Won Opps / T"        IS NULL
    AND "Total Opps / A" = 0  AND "Total Opps / T"      IS NULL
    AND "Trials / A" = 0      AND "Trials / T"          IS NULL
  )
)

SELECT *
FROM agg
