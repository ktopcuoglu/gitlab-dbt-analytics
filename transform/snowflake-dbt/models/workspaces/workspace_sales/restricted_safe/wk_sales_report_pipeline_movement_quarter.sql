{{ config(alias='report_pipeline_movement_quarter') }}

WITH sfdc_opportunity_snapshot_history_xf AS (

  SELECT *
  FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
  WHERE is_deleted = 0
    AND is_edu_oss = 0

), sfdc_opportunity_xf AS (
  
  SELECT 
        opportunity_id,
        close_fiscal_quarter_date,
        stage_name,
        is_won,
        is_lost,
        is_open,
        is_renewal,
        order_type_stamped,
        sales_qualified_source,
        deal_category,
        deal_group,
        sales_team_cro_level,
        sales_team_rd_asm_level
  FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  
  WHERE is_deleted = 0
    AND is_edu_oss = 0

), today_date AS (
  
   SELECT DISTINCT first_day_of_fiscal_quarter AS current_fiscal_quarter_date,
                   fiscal_quarter_name_fy      AS current_fiscal_quarter_name,
                   90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS current_day_of_fiscal_quarter_normalised
   FROM {{ ref('wk_sales_date_details') }} 
   WHERE date_actual = CURRENT_DATE 

), pipeline_type_start_ids AS (

    SELECT 
      opportunity_id,
      snapshot_fiscal_quarter_date,
      max(snapshot_day_of_fiscal_quarter_normalised) AS max_snapshot_day_of_fiscal_quarter_normalised,
      min(snapshot_day_of_fiscal_quarter_normalised) AS min_snapshot_day_of_fiscal_quarter_normalised
    FROM sfdc_opportunity_snapshot_history_xf        
    WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot
      AND stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
      AND snapshot_day_of_fiscal_quarter_normalised <= 5
      -- exclude web direct purchases
      AND is_web_portal_purchase = 0
    GROUP BY 1,2

), pipeline_type_web_purchase_ids AS (

    SELECT 
      opportunity_id,
      snapshot_fiscal_quarter_date,
      max(snapshot_day_of_fiscal_quarter_normalised) AS max_snapshot_day_of_fiscal_quarter_normalised,
      min(snapshot_day_of_fiscal_quarter_normalised) AS min_snapshot_day_of_fiscal_quarter_normalised
    FROM sfdc_opportunity_snapshot_history_xf        
    WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot
      AND stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
      -- include web direct purchases
      AND is_web_portal_purchase = 1
    GROUP BY 1,2

), pipeline_type_quarter_start AS (
    
    -- create a list of opties and min snapshot day to identify all the opties that should be flagged as starting in the first 5 days
    SELECT 
        starting.opportunity_id,
        starting.snapshot_fiscal_quarter_date,
        starting.close_fiscal_quarter_date       AS starting_close_fiscal_quarter_date,
        starting.close_date                      AS starting_close_date,
        starting.forecast_category_name          AS starting_forecast_category,
        starting.net_arr                         AS starting_net_arr,
        starting.booked_net_arr                  AS starting_booked_net_arr,
        starting.stage_name                      AS starting_stage,
        starting.snapshot_date                   AS starting_snapshot_date,
        starting.is_won                          AS starting_is_won,
        starting.is_open                         AS starting_is_open,
        starting.is_lost                         AS starting_is_lost
    FROM sfdc_opportunity_snapshot_history_xf starting
      INNER JOIN  pipeline_type_start_ids  starting_list
        ON starting.opportunity_id = starting_list.opportunity_id
          AND starting.snapshot_fiscal_quarter_date = starting_list.snapshot_fiscal_quarter_date
          AND starting.snapshot_day_of_fiscal_quarter_normalised = starting_list.max_snapshot_day_of_fiscal_quarter_normalised
    WHERE starting.snapshot_fiscal_quarter_date = starting.close_fiscal_quarter_date -- closing in the same quarter of the snapshot

), pipeline_type_quarter_created AS (

    SELECT 
        created.opportunity_id,
        created.pipeline_created_fiscal_quarter_date,
        min(created.snapshot_date)                      AS created_snapshot_date
    FROM sfdc_opportunity_snapshot_history_xf created
    LEFT JOIN pipeline_type_quarter_start starting
      ON starting.opportunity_id = created.opportunity_id
      AND starting.snapshot_fiscal_quarter_date = created.snapshot_fiscal_quarter_date
    LEFT JOIN pipeline_type_web_purchase_ids web
      ON web.opportunity_id = created.opportunity_id
      AND web.snapshot_fiscal_quarter_date = created.snapshot_fiscal_quarter_date
    WHERE created.stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
      -- pipeline created same quarter
      AND created.snapshot_fiscal_quarter_date = created.pipeline_created_fiscal_quarter_date
      AND created.is_eligible_created_pipeline_flag = 1
      AND created.pipeline_created_fiscal_quarter_date = created.close_fiscal_quarter_date
      -- not already flagged as starting pipeline
      AND starting.opportunity_id IS NULL  
      AND web.opportunity_id IS NULL
    GROUP BY 1, 2

), pipeline_type_pulled_in AS (

    SELECT 
        pull.opportunity_id,
        pull.snapshot_fiscal_quarter_date,
        min(pull.snapshot_date)            AS pulled_in_snapshot_date
    FROM sfdc_opportunity_snapshot_history_xf pull
    LEFT JOIN pipeline_type_quarter_start pipe_start
      ON pipe_start.opportunity_id = pull.opportunity_id
      AND pipe_start.snapshot_fiscal_quarter_date = pull.snapshot_fiscal_quarter_date
    LEFT JOIN pipeline_type_quarter_created pipe_created
      ON pipe_created.opportunity_id = pull.opportunity_id
      AND pipe_created.pipeline_created_fiscal_quarter_date = pull.snapshot_fiscal_quarter_date
    LEFT JOIN pipeline_type_web_purchase_ids web
      ON web.opportunity_id = pull.opportunity_id
      AND web.snapshot_fiscal_quarter_date = pull.snapshot_fiscal_quarter_date
    WHERE pull.stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
      AND pull.snapshot_fiscal_quarter_date = pull.close_fiscal_quarter_date
      AND pipe_start.opportunity_id IS NULL
      AND pipe_created.opportunity_id IS NULL  
      AND web.opportunity_id IS NULL
    GROUP BY 1, 2

), pipeline_type_quarter_end AS (

    SELECT 
        opportunity_id,
        snapshot_fiscal_quarter_date,
        close_fiscal_quarter_date       AS end_close_fiscal_quarter_date,
        close_date                      AS end_close_date,
        forecast_category_name          AS end_forecast_category,
        net_arr                         AS end_net_arr,
        booked_net_arr                  AS end_booked_net_arr,
        stage_name                      AS end_stage,
        stage_category                  AS end_stage_category,
        is_won                          AS end_is_won,
        is_open                         AS end_is_open,
        is_lost                         AS end_is_lost
    FROM sfdc_opportunity_snapshot_history_xf        
    WHERE (snapshot_day_of_fiscal_quarter_normalised = 90 
          OR snapshot_date = CURRENT_DATE)

), pipeline_type AS (

  SELECT 
        opp_snap.opportunity_id,
        opp_snap.close_fiscal_quarter_date,
        opp_snap.close_fiscal_quarter_name,

        pipe_start.starting_forecast_category,
        pipe_start.starting_net_arr,
        pipe_start.starting_stage,
        pipe_start.starting_close_date,
        pipe_start.starting_is_open,
        pipe_start.starting_is_won,
        pipe_start.starting_is_lost,

        pipe_end.end_forecast_category,
        pipe_end.end_net_arr,
        pipe_end.end_booked_net_arr,
        pipe_end.end_stage,
        pipe_end.end_stage_category,
        pipe_end.end_is_open,
        pipe_end.end_is_won,
        pipe_end.end_is_lost,
        pipe_end.end_close_date,

        -- pipeline type, identifies if the opty was there at the begging of the quarter or not
        CASE
          WHEN pipe_start.opportunity_id IS NOT NULL
            THEN '1. Starting'
          WHEN web.opportunity_id IS NOT NULL
            THEN '4. Web Direct'
          WHEN pipe_created.opportunity_id IS NOT NULL
            THEN '2. Created & Landed'
          WHEN pipe_pull.opportunity_id IS NOT NULL
            THEN '3. Pulled in'
          ELSE Null
        END                                                         AS pipeline_type,

        -- created pipe
        MAX(pipe_created.created_snapshot_date)                     AS pipeline_created_snapshot_date,

        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.net_arr
                  ELSE NULL
            END)                                                    AS pipeline_created_net_arr,
        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.stage_name
                  ELSE ''
            END)                                                    AS pipeline_created_stage,

        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.forecast_category_name
                  ELSE ''
            END)                                                    AS pipeline_created_forecast_category,
        
        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.close_date
                  ELSE Null
            END)                                                    AS pipeline_created_close_date,

        -- pulled in pipe
        
        MAX(CASE
              WHEN pipe_pull.pulled_in_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.net_arr
                  ELSE NULL
            END)                                                    AS pipeline_pull_net_arr,

        ----

        MIN(opp_snap.snapshot_day_of_fiscal_quarter_normalised)     AS min_snapshot_day_of_fiscal_quarter_normalised,
        MAX(opp_snap.snapshot_day_of_fiscal_quarter_normalised)     AS max_snapshot_day_of_fiscal_quarter_normalised,

        MIN(opp_snap.snapshot_date)                                 AS min_snapshot_date,
        MAX(opp_snap.snapshot_date)                                 AS max_snapshot_date,

        MIN(opp_snap.close_date)                                    AS min_close_date,
        MAX(opp_snap.close_date)                                    AS max_close_date,

        MIN(opp_snap.net_arr)                                       AS min_net_arr,
        MAX(opp_snap.net_arr)                                       AS max_net_arr,

        MIN(opp_snap.stage_name)                                    AS min_stage_name,
        MAX(opp_snap.stage_name)                                    AS max_stage_name
  FROM sfdc_opportunity_snapshot_history_xf opp_snap
  -- starting pipeline
  LEFT JOIN pipeline_type_quarter_start pipe_start
      ON pipe_start.opportunity_id = opp_snap.opportunity_id
      AND pipe_start.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
  -- end pipeline
  LEFT JOIN pipeline_type_quarter_end pipe_end
      ON pipe_end.opportunity_id = opp_snap.opportunity_id
      AND pipe_end.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
  -- created pipeline
  LEFT JOIN pipeline_type_quarter_created pipe_created
      ON pipe_created.opportunity_id = opp_snap.opportunity_id
      AND pipe_created.pipeline_created_fiscal_quarter_date = opp_snap.close_fiscal_quarter_date
  -- pulled in pipeline
  LEFT JOIN pipeline_type_pulled_in pipe_pull
      ON pipe_pull.opportunity_id = opp_snap.opportunity_id
      AND pipe_pull.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
  -- web direct pipeline
  LEFT JOIN pipeline_type_web_purchase_ids web
      ON web.opportunity_id = opp_snap.opportunity_id
      AND web.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
  -- closing in the same quarter of the snapshot
  WHERE opp_snap.snapshot_fiscal_quarter_date = opp_snap.close_fiscal_quarter_date 
    -- Exclude duplicate deals that were not created or started within the quarter
    AND (
          pipe_start.opportunity_id IS NOT NULL
          OR pipe_created.opportunity_id IS NOT NULL        
          OR pipe_pull.opportunity_id IS NOT NULL
          OR web.opportunity_id IS NOT NULL   
        )
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20

-- last day within snapshot quarter of a particular opportunity
), pipeline_last_day_in_snapshot_quarter AS (

  SELECT 
    pipeline_type.opportunity_id,
    pipeline_type.close_fiscal_quarter_date,
    history.stage_name,
    history.forecast_category_name,
    history.net_arr,
    history.booked_net_arr
  FROM pipeline_type
    INNER JOIN sfdc_opportunity_snapshot_history_xf history
      ON history.opportunity_id = pipeline_type.opportunity_id
      AND history.snapshot_date = pipeline_type.max_snapshot_date


), report_opportunity_pipeline_type AS (

  SELECT 
  
        pipe.opportunity_id,
        -- descriptive cuts
        opty.order_type_stamped,
        opty.sales_qualified_source,
        opty.deal_category,
        opty.deal_group,
        opty.sales_team_cro_level,
        opty.sales_team_rd_asm_level,
        -- pipeline fields
        pipe.close_fiscal_quarter_date      AS report_fiscal_quarter_date,
        pipe.close_fiscal_quarter_name      AS report_fiscal_quarter_name,
        pipe.pipeline_type,

        CASE 
          WHEN pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
            THEN 1
          ELSE 0
        END                                                                     AS is_closed_in_quarter_flag,
        CASE 
            WHEN pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                AND opty.order_type_stamped NOT IN ('4. Contraction')
                AND opty.is_won = 1
                    THEN '1. Closed Won'
            -- the close date for churned deals is updated to the last day before renewal
            WHEN opty.order_type_stamped IN ('5. Churn - Partial','6. Churn - Final')
                    THEN '6. Churned'
            WHEN opty.order_type_stamped IN ('4. Contraction')
                    THEN '5. Contraction'
            WHEN pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                AND opty.is_lost = 1
                    THEN '4. Closed Lost'
            WHEN pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                AND opty.is_open = 1
                    THEN '7. Open'
            WHEN pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
                AND opty.stage_name IN ('9-Unqualified','10-Duplicate','Unqualified')
                    THEN '8. Duplicate / Unqualified'
            WHEN pipe.close_fiscal_quarter_date <> opty.close_fiscal_quarter_date
                AND pipe.max_snapshot_day_of_fiscal_quarter_normalised >= 75
                    THEN '2. Slipped'
            WHEN pipe.close_fiscal_quarter_date <> opty.close_fiscal_quarter_date
                AND pipe.max_snapshot_day_of_fiscal_quarter_normalised < 75
                    THEN '3. Pushed Out' 
          ELSE '9. Other'
        END                                                                     AS pipe_resolution,

        CASE
          WHEN pipe_resolution IN ('1. Closed Won','4. Closed Lost','8. Duplicate / Unqualified') 
            THEN pipe.end_close_date
          WHEN pipe_resolution IN ('5. Contraction','6. Churned')
            AND pipe.close_fiscal_quarter_date = opty.close_fiscal_quarter_date
            THEN pipe.end_close_date
          WHEN pipe_resolution IN ('2. Slipped', '3. Pushed Out', '7. Open','9. Other')
            THEN pipe.max_snapshot_date
          ELSE null
        END                                                                     AS pipe_resolution_date,

        -- basic net arr

        COALESCE(pipe.starting_net_arr,pipe.pipeline_created_net_arr,pipe.pipeline_pull_net_arr,0)    AS quarter_start_net_arr,
        COALESCE(pipe.starting_stage,pipe.pipeline_created_stage)                                     AS quarter_start_stage,
        COALESCE(pipe.starting_forecast_category,pipe.pipeline_created_forecast_category)             AS quarter_start_forecast_category,
        COALESCE(pipe.starting_close_date,pipe.pipeline_created_close_date)                           AS quarter_start_close_date,

        -- last day the opportunity was closing in quarter
        last_day.net_arr                  AS last_day_net_arr,                                                       
        last_day.booked_net_arr           AS last_day_booked_net_arr,
        last_day.stage_name               AS last_day_stage_name,
        last_day.forecast_category_name   AS last_day_forecast_category,

        -- last day of the quarter, at this point the deal might not be closing
        -- on the quarter
        pipe.end_booked_net_arr            AS quarter_end_booked_net_arr,                                                                                                   
        pipe.end_net_arr                   AS quarter_end_net_arr,
        pipe.end_stage                     AS quarter_end_stage,  
        pipe.end_stage_category            AS quarter_end_stage_category,                                              
        pipe.end_forecast_category         AS quarter_end_forecast_category,
        pipe.end_close_date                AS quarter_end_close_date,
        pipe.end_is_won                    AS quarter_end_is_won,
        pipe.end_is_lost                   AS quarter_end_is_lost,
        pipe.end_is_open                   AS quarter_end_is_open,
        
        opty.is_renewal                    AS is_renewal,

        last_day.net_arr - quarter_start_net_arr  AS within_quarter_delta_net_arr,

        ----------
        -- extra fields for trouble shooting
        
        pipe.min_snapshot_day_of_fiscal_quarter_normalised,
        pipe.max_snapshot_day_of_fiscal_quarter_normalised,

        pipe.min_snapshot_date,
        pipe.max_snapshot_date,

        pipe.min_close_date,
        pipe.max_close_date,

        pipe.min_net_arr,
        pipe.max_net_arr,

        pipe.min_stage_name,
        pipe.max_stage_name,

        ----------
        CURRENT_DATE                                                    AS last_updated_at


  FROM pipeline_type pipe
  CROSS JOIN today_date 
  INNER JOIN sfdc_opportunity_xf opty
      ON opty.opportunity_id = pipe.opportunity_id
  LEFT JOIN pipeline_last_day_in_snapshot_quarter last_day
    ON last_day.opportunity_id = pipe.opportunity_id
    AND pipe.close_fiscal_quarter_date = last_day.close_fiscal_quarter_date
) 

SELECT *
FROM report_opportunity_pipeline_type