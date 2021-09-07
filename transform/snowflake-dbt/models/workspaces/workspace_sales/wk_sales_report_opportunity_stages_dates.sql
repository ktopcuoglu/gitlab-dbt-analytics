 {{ config(alias='report_opportunity_stages_dates') }}

WITH sfdc_opportunity_field_history AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_field_history_source')}}

)
, sfdc_opportunity_xf AS (

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
       MAX(CASE WHEN stage_name = '0-Pending Acceptance' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_0_date,
       MAX(CASE WHEN stage_name = '1-Discovery' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_1_date,
       MAX(CASE WHEN stage_name = '2-Scoping' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_2_date,
       MAX(CASE WHEN stage_name = '3-Technical Evaluation' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_3_date,
       MAX(CASE WHEN stage_name = '4-Proposal' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_4_date,
       MAX(CASE WHEN stage_name = '5-Negotiating' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_5_date,
       MAX(CASE WHEN stage_name = '6-Awaiting Signature' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_6_date,
       MAX(CASE WHEN stage_name = '7-Closing' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_7_date,
       MAX(CASE WHEN stage_name = '8-Closed Lost' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_8_lost_date,
       MAX(CASE WHEN stage_name = 'Closed Won' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_8_won_date,
       MAX(CASE WHEN stage_name = '9-Unqualified' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_9_date,
       MAX(CASE WHEN stage_name = '10-Duplicate' 
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_10_date,
       MAX(CASE WHEN stage_name IN ('8-Closed Lost','Closed Won','10-Duplicate','9-Unqualified') 
          THEN min_stage_date
       ELSE NULL END)                  AS max_closed_stage_date

  FROM combined
  GROUP BY 1
  
)
, pre_final AS (

SELECT  
        c.opportunity_id,
        o.stage_name,
        o.close_date,
        o.sales_team_cro_level,
        o.sales_team_rd_asm_level,
        -- adjusted dates for throughput analysis
        -- missing stage dates are completed using the next available stage date, up to a closed date
        coalesce(c.min_stage_0_date,o.created_date) as stage_0_date,
        coalesce(c.min_stage_1_date,c.min_stage_2_date,c.min_stage_3_date,c.min_stage_4_date,c.min_stage_5_date,c.min_stage_6_date,c.min_stage_7_date,c.min_stage_8_won_date)  AS stage_1_date,
        coalesce(c.min_stage_2_date,c.min_stage_3_date,c.min_stage_4_date,c.min_stage_5_date,c.min_stage_6_date,c.min_stage_7_date,c.min_stage_8_won_date)                     AS stage_2_date,
        coalesce(c.min_stage_3_date,c.min_stage_4_date,c.min_stage_5_date,c.min_stage_6_date,c.min_stage_7_date,c.min_stage_8_won_date)                                        AS stage_3_date,
        coalesce(c.min_stage_4_date,c.min_stage_5_date,c.min_stage_6_date,c.min_stage_7_date,c.min_stage_8_won_date)                                                           AS stage_4_date,
        coalesce(c.min_stage_5_date,c.min_stage_6_date,c.min_stage_7_date,c.min_stage_8_won_date)                                                                              AS stage_5_date,
        coalesce(c.min_stage_6_date,c.min_stage_7_date,min_stage_8_won_date)                                                                                                   AS stage_6_date,
        coalesce(c.min_stage_7_date,c.min_stage_8_won_date)                                                                                                                    AS stage_7_date,
        c.min_stage_8_lost_date                     AS stage_8_lost_date,
        c.min_stage_8_won_date                      AS stage_8_won_date,
        c.min_stage_9_date                          AS stage_9_date,
        c.min_stage_10_date                         AS stage_10_date,
        c.max_closed_stage_date                     AS stage_closed_date,
        
        -- unadjusted fields
        c.min_stage_0_date,
        c.min_stage_1_date,
        c.min_stage_2_date,
        c.min_stage_3_date,
        c.min_stage_4_date,
        c.min_stage_5_date,
        c.min_stage_6_date,
        c.min_stage_7_date
    FROM pivoted_combined c
    INNER JOIN sfdc_opportunity_xf o
      ON o.opportunity_id = c.opportunity_id
)
, final AS (

SELECT  
        *,

        -- was stage skipped flag
        CASE
            WHEN min_stage_0_date IS NULL
                AND stage_0_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_0_skipped_flag,
        CASE
            WHEN min_stage_1_date IS NULL
                AND stage_1_date IS NOT NULL            
                THEN 1
            ELSE 0 
        END AS was_stage_1_skipped_flag,
        CASE
            WHEN min_stage_2_date IS NULL
                AND stage_2_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_2_skipped_flag,
        CASE
            WHEN min_stage_3_date IS NULL
                AND stage_3_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_3_skipped_flag,
        CASE
            WHEN min_stage_4_date IS NULL
                AND stage_4_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_4_skipped_flag,        
        CASE
            WHEN min_stage_5_date IS NULL
                AND stage_5_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_5_skipped_flag,
        CASE
            WHEN min_stage_6_date IS NULL
                AND stage_6_date IS NOT NULL
                THEN 1
            ELSE 0 
        END AS was_stage_6_skipped_flag,
        CASE
            WHEN min_stage_7_date IS NULL
                AND stage_7_date IS NOT  NULL
                THEN 1
            ELSE 0 
        END AS was_stage_7_skipped_flag
 
FROM pre_final 

)

SELECT *
FROM final

