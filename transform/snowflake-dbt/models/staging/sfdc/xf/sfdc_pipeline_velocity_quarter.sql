/*
Original issue: 
https://gitlab.com/gitlab-data/analytics/-/issues/6069

*/

WITH sfdc_opportunity_snapshot_history_xf AS (
    
    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history_xf') }}
    -- remove lost & deleted deals
    WHERE stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified')
      AND is_deleted = 0

), final AS (

    SELECT
      snapshot_date,
      close_fiscal_quarter,
      close_fiscal_quarter_date,
      close_fiscal_year,
      order_type_stamped,
      adj_ultimate_parent_sales_segment                       AS sales_segment,
      stage_name_3plus,
      stage_name_4plus,
      is_excluded_flag,
      stage_name,
      forecast_category_name,
      COUNT(DISTINCT opportunity_id)                          AS opps,
      SUM(net_iacv)                                           AS net_iacv,
      SUM(churn_only)                                         AS churn_only,
      SUM(forecasted_iacv)                                    AS forecasted_iacv,
      SUM(total_contract_value)                               AS tcv
    FROM sfdc_opportunity_snapshot_history_xf 
    WHERE 
      -- 2 quarters before start and full quarter, total rolling 9 months at end of quarter
      -- till end of quarter
      snapshot_date <= DATEADD(month,3,close_fiscal_quarter_date)
      -- 2 quarters before start
      AND snapshot_date >= DATEADD(month,-6,close_fiscal_quarter_date)
      -- remove forecast category Omitted
      AND forecast_category_name != 'Omitted'
    {{ dbt_utils.group_by(n=11) }}
)

SELECT *
FROM final