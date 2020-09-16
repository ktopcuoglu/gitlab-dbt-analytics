
/*
Original issue: 
https://gitlab.com/gitlab-data/analytics/-/issues/6069

Questions:
    - What is the purpose of the level2 VP filter?

*/ 
WITH sfdc_opportunity_snapshot_history_xf AS (
    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history_xf') }}
    -- remove lost & deleted deals
    WHERE stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified')
        AND is_deleted = 0
) 
-- This portion is specfic for the Forecast Model
SELECT  snapshot_date,
        close_month,
        close_fiscal_year,
        forecast_category_name,                  
        owner_id,
        opportunity_owner_manager,
        tsp_region,
        tsp_sub_region,
        stage_name,
        segment,
        --aggregations
        COUNT(DISTINCT opportunity_id)                                      AS opps,
        SUM(net_iacv)                                                       AS net_iacv,
        -- why closed_lost counts as pipeline?
        SUM(forecasted_iacv)                                                AS forecasted_iacv
FROM sfdc_opportunity_snapshot_history_xf
-- filter only deals at the VP level 2 hierarchy
WHERE is_lvl_2_vp_flag = 1
        -- one month before
        AND snapshot_date >= dateadd(month,-1, close_month)
        -- till end of the month
        AND snapshot_date <= dateadd(month, 1, close_month)
GROUP BY 1,2,3,4,5,6,7,8,9,10
