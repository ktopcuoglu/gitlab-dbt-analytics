{{ config(
    tags=["mnpi_exception"]
) }}

 {{ config(alias='report_opportunity_stages_dates') }}

 --TODO
 -- Check out for deals created in a stage that is not 0, use the creation date

WITH sfdc_opportunity_field_history AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_field_history_source')}}

), date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }} 

), sfdc_opportunity_xf AS (

  SELECT *
  FROM {{ ref('wk_sales_sfdc_opportunity_xf')}}

)
-- after every stage change, as it is a tracked field
-- a record would be created in the field history table
, history_base AS (

  SELECT  
          opportunity_id,
          replace(replace(replace(replace(replace(new_value_string,'2-Developing','2-Scoping'),'7 - Closing','7-Closing'),'Developing','2-Scoping'),'Closed Lost','8-Closed Lost'),'8-8-Closed Lost','8-Closed Lost') AS new_value_string,
          min(field_modified_at::date) AS min_stage_date

  FROM sfdc_opportunity_field_history
  WHERE opportunity_field = 'stagename'
  GROUP BY 1,2

-- just created opportunities won't have any historical record
-- next CTE accounts for them
), opty_base AS (

 SELECT
    o.opportunity_id,
    o.stage_name,
    o.created_date AS min_stage_date
 FROM sfdc_opportunity_xf o
 LEFT JOIN (SELECT DISTINCT opportunity_id FROM history_base) h
    ON h.opportunity_id = o.opportunity_id
 WHERE h.opportunity_id is null

), combined AS (

  SELECT opportunity_id,
      new_value_string AS stage_name,
      min_stage_date
  FROM history_base
  UNION
  SELECT opportunity_id,
      stage_name,
      min_stage_date
  FROM opty_base

), pivoted_combined AS (

  SELECT opportunity_id,
       MIN(CASE WHEN stage_name = '0-Pending Acceptance' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_0_date,
       MIN(CASE WHEN stage_name = '1-Discovery' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_1_date,
       MIN(CASE WHEN stage_name = '2-Scoping' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_2_date,
       MIN(CASE WHEN stage_name = '3-Technical Evaluation' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_3_date,
       MIN(CASE WHEN stage_name = '4-Proposal' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_4_date,
       MIN(CASE WHEN stage_name = '5-Negotiating' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_5_date,
       MIN(CASE WHEN stage_name = '6-Awaiting Signature' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_6_date,
       MIN(CASE WHEN stage_name = '7-Closing' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_7_date,
       MIN(CASE WHEN stage_name = '8-Closed Lost' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_8_lost_date,
       MIN(CASE WHEN stage_name = 'Closed Won' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_8_won_date,
       MIN(CASE WHEN stage_name = '9-Unqualified' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_9_date,
       MIN(CASE WHEN stage_name = '10-Duplicate' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_10_date,
       MAX(CASE WHEN stage_name IN ('8-Closed Lost','Closed Won','10-Duplicate','9-Unqualified') 
          THEN min_stage_date
       ELSE NULL END)                  AS max_closed_stage_date,
       MAX(CASE WHEN stage_name IN ('8-Closed Lost','10-Duplicate','9-Unqualified') 
          THEN min_stage_date
       ELSE NULL END)                  AS max_closed_lost_unqualified_duplicate_date

  FROM combined
  GROUP BY 1
  
)
, pre_final AS (

SELECT  
        base.opportunity_id,
        opty.stage_name,
        opty.close_date,
        opty.sales_team_cro_level,
        opty.sales_team_rd_asm_level,
        -- adjusted dates for throughput analysis
        -- missing stage dates are completed using the next available stage date, up to a closed date
        coalesce(base.min_stage_0_date,opty.created_date) as stage_0_date,
        coalesce(base.min_stage_1_date,base.min_stage_2_date,base.min_stage_3_date,base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date)  AS stage_1_date,
        coalesce(base.min_stage_2_date,base.min_stage_3_date,base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date)                     AS stage_2_date,
        coalesce(base.min_stage_3_date,base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date)                                        AS stage_3_date,
        coalesce(base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date)                                                           AS stage_4_date,
        coalesce(base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date)                                                                              AS stage_5_date,
        coalesce(base.min_stage_6_date,base.min_stage_7_date,min_stage_8_won_date)                                                                                                   AS stage_6_date,
        coalesce(base.min_stage_7_date,base.min_stage_8_won_date)                                                                                                                    AS stage_7_date,
        base.min_stage_8_lost_date                     AS stage_8_lost_date,
        base.min_stage_8_won_date                      AS stage_8_won_date,
        base.min_stage_9_date                          AS stage_9_date,
        base.min_stage_10_date                         AS stage_10_date,
        base.max_closed_stage_date                     AS stage_closed_date,
        base.max_closed_lost_unqualified_duplicate_date AS stage_close_lost_unqualified_duplicate_date,
        
        -- unadjusted fields
        base.min_stage_0_date,
        base.min_stage_1_date,
        base.min_stage_2_date,
        base.min_stage_3_date,
        base.min_stage_4_date,
        base.min_stage_5_date,
        base.min_stage_6_date,
        base.min_stage_7_date
    FROM pivoted_combined base
    INNER JOIN sfdc_opportunity_xf opty
      ON opty.opportunity_id = base.opportunity_id
)
, final AS (

SELECT  
        base.*,

        -- was stage skipped flag
        CASE
            WHEN base.min_stage_0_date IS NULL
                AND base.stage_0_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_0_skipped_flag,
        CASE
            WHEN base.min_stage_1_date IS NULL
                AND base.stage_1_date IS NOT NULL            
                THEN 1
            ELSE 0 
        END AS was_stage_1_skipped_flag,
        CASE
            WHEN base.min_stage_2_date IS NULL
                AND base.stage_2_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_2_skipped_flag,
        CASE
            WHEN base.min_stage_3_date IS NULL
                AND base.stage_3_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_3_skipped_flag,
        CASE
            WHEN base.min_stage_4_date IS NULL
                AND base.stage_4_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_4_skipped_flag,        
        CASE
            WHEN base.min_stage_5_date IS NULL
                AND base.stage_5_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_5_skipped_flag,
        CASE
            WHEN base.min_stage_6_date IS NULL
                AND base.stage_6_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_6_skipped_flag,
        CASE
            WHEN base.min_stage_7_date IS NULL
                AND base.stage_7_date IS NOT  NULL
                THEN 1
            ELSE 0 
        END AS was_stage_7_skipped_flag,

        -- calculate age in stage
        DATEDIFF(day,coalesce(base.stage_1_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE), base.stage_0_date) AS days_in_stage_0,
        DATEDIFF(day,coalesce(base.stage_2_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE), base.stage_1_date) AS days_in_stage_1,
        DATEDIFF(day,coalesce(base.stage_3_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE), base.stage_2_date) AS days_in_stage_2,
        DATEDIFF(day,coalesce(base.stage_4_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE), base.stage_3_date) AS days_in_stage_3,
        DATEDIFF(day,coalesce(base.stage_5_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE), base.stage_4_date) AS days_in_stage_4,
        DATEDIFF(day,coalesce(base.stage_6_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE), base.stage_5_date) AS days_in_stage_5,
        DATEDIFF(day,coalesce(base.stage_7_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE), base.stage_6_date) AS days_in_stage_6,        
        DATEDIFF(day,coalesce(base.stage_closed_date,CURRENT_DATE), base.stage_7_date)                                             AS days_in_stage_7, 

        -- stage date helpers
        stage_0.fiscal_quarter_name_fy      AS stage_0_fiscal_quarter_name,
        stage_0.first_day_of_fiscal_quarter AS stage_0_fiscal_quarter_date,
        stage_0.fiscal_year                 AS stage_0_fiscal_year,
        
        stage_1.fiscal_quarter_name_fy      AS stage_1_fiscal_quarter_name,
        stage_1.first_day_of_fiscal_quarter AS stage_1_fiscal_quarter_date,
        stage_1.fiscal_year                 AS stage_1_fiscal_year,

        stage_2.fiscal_quarter_name_fy      AS stage_2_fiscal_quarter_name,
        stage_2.first_day_of_fiscal_quarter AS stage_2_fiscal_quarter_date,
        stage_2.fiscal_year                 AS stage_2_fiscal_year,

        stage_3.fiscal_quarter_name_fy      AS stage_3_fiscal_quarter_name,
        stage_3.first_day_of_fiscal_quarter AS stage_3_fiscal_quarter_date,
        stage_3.fiscal_year                 AS stage_3_fiscal_year,

        stage_4.fiscal_quarter_name_fy      AS stage_4_fiscal_quarter_name,
        stage_4.first_day_of_fiscal_quarter AS stage_4_fiscal_quarter_date,
        stage_4.fiscal_year                 AS stage_4_fiscal_year,

        stage_5.fiscal_quarter_name_fy      AS stage_5_fiscal_quarter_name,
        stage_5.first_day_of_fiscal_quarter AS stage_5_fiscal_quarter_date,
        stage_5.fiscal_year                 AS stage_5_fiscal_year,

        stage_6.fiscal_quarter_name_fy      AS stage_6_fiscal_quarter_name,
        stage_6.first_day_of_fiscal_quarter AS stage_6_fiscal_quarter_date,
        stage_6.fiscal_year                 AS stage_6_fiscal_year,

        stage_7.fiscal_quarter_name_fy      AS stage_7_fiscal_quarter_name,
        stage_7.first_day_of_fiscal_quarter AS stage_7_fiscal_quarter_date,
        stage_7.fiscal_year                 AS stage_7_fiscal_year

FROM pre_final base
  LEFT JOIN  date_details stage_0
    ON stage_0.date_actual = base.stage_0_date::date
  LEFT JOIN  date_details stage_1
    ON stage_1.date_actual = base.stage_1_date::date
  LEFT JOIN  date_details stage_2
    ON stage_2.date_actual = base.stage_2_date::date
  LEFT JOIN  date_details stage_3
    ON stage_3.date_actual = base.stage_3_date::date
  LEFT JOIN  date_details stage_4
    ON stage_4.date_actual = base.stage_4_date::date
  LEFT JOIN  date_details stage_5
    ON stage_5.date_actual = base.stage_5_date::date
  LEFT JOIN  date_details stage_6
    ON stage_6.date_actual = base.stage_6_date::date
  LEFT JOIN  date_details stage_7
    ON stage_7.date_actual = base.stage_7_date::date

)

SELECT *
FROM final

