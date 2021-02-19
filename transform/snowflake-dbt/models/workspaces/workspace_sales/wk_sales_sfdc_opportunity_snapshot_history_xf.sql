{{ config(alias='sfdc_opportunity_snapshot_history_xf') }}

WITH date_details AS (

    SELECT
      *,
      90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS day_of_fiscal_quarter_normalised,
      12-floor((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)/7))  AS week_of_fiscal_quarter_normalised,
      CASE 
        WHEN ((DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)-6) % 7 = 0 
                OR date_actual = first_day_of_fiscal_quarter) 
          THEN 1 
          ELSE 0 
      END                                                                   AS first_day_of_fiscal_quarter_week_normalised 
    FROM {{ ref('date_details') }} 
    ORDER BY 1 DESC

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('sfdc_accounts_xf')}} 

), sfdc_opportunity_snapshot_history AS (

    SELECT 
      sfdc_opportunity_snapshot_history.valid_from,
      sfdc_opportunity_snapshot_history.valid_to,
      sfdc_opportunity_snapshot_history.is_currently_valid,
      sfdc_opportunity_snapshot_history.opportunity_snapshot_id,
      sfdc_opportunity_snapshot_history.account_id,
      sfdc_opportunity_snapshot_history.opportunity_id,
      sfdc_opportunity_snapshot_history.opportunity_name,
      sfdc_opportunity_snapshot_history.owner_id,
      sfdc_opportunity_snapshot_history.business_type,
      sfdc_opportunity_snapshot_history.close_date,
      sfdc_opportunity_snapshot_history.created_date,
      sfdc_opportunity_snapshot_history.deployment_preference,
      sfdc_opportunity_snapshot_history.generated_source,
      sfdc_opportunity_snapshot_history.lead_source,
      sfdc_opportunity_snapshot_history.merged_opportunity_id,
      sfdc_opportunity_snapshot_history.opportunity_owner,
 
      sfdc_opportunity_snapshot_history.opportunity_owner_department,
      sfdc_opportunity_snapshot_history.opportunity_sales_development_representative,
      sfdc_opportunity_snapshot_history.opportunity_business_development_representative,
      sfdc_opportunity_snapshot_history.opportunity_development_representative,

      --sfdc_opportunity_snapshot_history.order_type,
      --sfdc_opportunity_snapshot_history.opportunity_owner_team,
      --sfdc_opportunity_snapshot_history.opportunity_owner_manager,
      --sfdc_opportunity_snapshot_history.account_owner_team_stamped,
      sfdc_opportunity_snapshot_history.parent_segment,
      sfdc_opportunity_snapshot_history.sales_accepted_date,
      sfdc_opportunity_snapshot_history.sales_path,
      sfdc_opportunity_snapshot_history.sales_qualified_date,
      sfdc_opportunity_snapshot_history.sales_segment,
      sfdc_opportunity_snapshot_history.sales_type,
      sfdc_opportunity_snapshot_history.net_new_source_categories,
      sfdc_opportunity_snapshot_history.source_buckets,
      sfdc_opportunity_snapshot_history.stage_name,

      sfdc_opportunity_snapshot_history.acv,
      sfdc_opportunity_snapshot_history.closed_deals,
      sfdc_opportunity_snapshot_history.competitors,
      sfdc_opportunity_snapshot_history.critical_deal_flag,
      sfdc_opportunity_snapshot_history.deal_size,
      sfdc_opportunity_snapshot_history.forecast_category_name,
      sfdc_opportunity_snapshot_history.forecasted_iacv,
      sfdc_opportunity_snapshot_history.iacv_created_date,
      sfdc_opportunity_snapshot_history.incremental_acv,
      sfdc_opportunity_snapshot_history.invoice_number,

      -- logic needs to be added here once the oppotunity category fields is merged
      -- https://gitlab.com/gitlab-data/analytics/-/issues/7888
      CASE
        WHEN sfdc_opportunity_snapshot_history.opportunity_category IN ('Credit', 'Decommission','Decommissioned')
          THEN 1
        ELSE 0
      END                                                          AS is_refund,
      --sfdc_opportunity_snapshot_history.is_refund,

      sfdc_opportunity_snapshot_history.is_downgrade,
      sfdc_opportunity_snapshot_history.is_swing_deal,
      sfdc_opportunity_snapshot_history.net_incremental_acv,
      sfdc_opportunity_snapshot_history.nrv,
      sfdc_opportunity_snapshot_history.primary_campaign_source_id,
      sfdc_opportunity_snapshot_history.probability,
      sfdc_opportunity_snapshot_history.professional_services_value,
      sfdc_opportunity_snapshot_history.pushed_count,
      sfdc_opportunity_snapshot_history.reason_for_loss,
      sfdc_opportunity_snapshot_history.reason_for_loss_details,
      sfdc_opportunity_snapshot_history.refund_iacv,
      sfdc_opportunity_snapshot_history.downgrade_iacv,
      sfdc_opportunity_snapshot_history.renewal_acv,
      sfdc_opportunity_snapshot_history.renewal_amount,
      sfdc_opportunity_snapshot_history.sales_qualified_source,
      sfdc_opportunity_snapshot_history.segment,
      sfdc_opportunity_snapshot_history.solutions_to_be_replaced,
      sfdc_opportunity_snapshot_history.total_contract_value,
      sfdc_opportunity_snapshot_history.upside_iacv,
      sfdc_opportunity_snapshot_history.upside_swing_deal_iacv,
      sfdc_opportunity_snapshot_history.is_web_portal_purchase,
      sfdc_opportunity_snapshot_history.opportunity_term,
      
      sfdc_opportunity_snapshot_history.net_arr             AS raw_net_arr,
      
      sfdc_opportunity_snapshot_history.user_segment_stamped,
      sfdc_opportunity_snapshot_history.user_region_stamped,
      sfdc_opportunity_snapshot_history.user_area_stamped,
      sfdc_opportunity_snapshot_history.user_geo_stamped,
      
      sfdc_opportunity_snapshot_history.arr_basis,
      sfdc_opportunity_snapshot_history.arr,
      sfdc_opportunity_snapshot_history.amount,
      sfdc_opportunity_snapshot_history.recurring_amount,
      sfdc_opportunity_snapshot_history.true_up_amount,
      sfdc_opportunity_snapshot_history.proserv_amount,
      sfdc_opportunity_snapshot_history.other_non_recurring_amount,
      sfdc_opportunity_snapshot_history.subscription_start_date,
      sfdc_opportunity_snapshot_history.subscription_end_date,
      sfdc_opportunity_snapshot_history.cp_champion,
      sfdc_opportunity_snapshot_history.cp_close_plan,
      sfdc_opportunity_snapshot_history.cp_competition,
      sfdc_opportunity_snapshot_history.cp_decision_criteria,
      sfdc_opportunity_snapshot_history.cp_decision_process,
      sfdc_opportunity_snapshot_history.cp_economic_buyer,
      sfdc_opportunity_snapshot_history.cp_identify_pain,
      sfdc_opportunity_snapshot_history.cp_metrics,
      sfdc_opportunity_snapshot_history.cp_risks,
      sfdc_opportunity_snapshot_history.cp_use_cases,
      sfdc_opportunity_snapshot_history.cp_value_driver,
      sfdc_opportunity_snapshot_history.cp_why_do_anything_at_all,
      sfdc_opportunity_snapshot_history.cp_why_gitlab,
      sfdc_opportunity_snapshot_history.cp_why_now,
      sfdc_opportunity_snapshot_history._last_dbt_run,
      sfdc_opportunity_snapshot_history.is_deleted,
      sfdc_opportunity_snapshot_history.last_activity_date,
      sfdc_opportunity_snapshot_history.record_type_id,
      sfdc_opportunity_snapshot_history.opportunity_category,


      --date helpers

      sfdc_opportunity_snapshot_history.date_actual              AS snapshot_date,  
      snapshot_date.first_day_of_month                           AS snapshot_date_month,
      snapshot_date.fiscal_year                                  AS snapshot_fiscal_year,
      snapshot_date.fiscal_quarter_name_fy                       AS snapshot_fiscal_quarter_name,
      snapshot_date.first_day_of_fiscal_quarter                  AS snapshot_fiscal_quarter_date,
      snapshot_date.day_of_fiscal_quarter_normalised             AS snapshot_day_of_fiscal_quarter_normalised,
      
      close_date_detail.first_day_of_month                       AS close_date_month,
      close_date_detail.fiscal_year                              AS close_fiscal_year,
      close_date_detail.fiscal_quarter_name_fy                   AS close_fiscal_quarter_name,
      close_date_detail.first_day_of_fiscal_quarter              AS close_fiscal_quarter_date,

      created_date_detail.first_day_of_month                     AS created_date_month,
      created_date_detail.fiscal_year                            AS created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                 AS created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter            AS created_fiscal_quarter_date,

      iacv_created_date.first_day_of_month                       AS iacv_created_date_month,
      iacv_created_date.fiscal_year                              AS iacv_created_fiscal_year,
      iacv_created_date.fiscal_quarter_name_fy                   AS iacv_created_fiscal_quarter_name,
      iacv_created_date.first_day_of_fiscal_quarter              AS iacv_created_fiscal_quarter_date,

      -- this fields might change, isolating the field used from the purpose
      -- alternative is future net_arr_created_date
      created_date_detail.first_day_of_month                     AS pipeline_created_date_month,
      created_date_detail.fiscal_year                            AS pipeline_created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                 AS pipeline_created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter            AS pipeline_created_fiscal_quarter_date

    FROM {{ref('sfdc_opportunity_snapshot_history')}}
    INNER JOIN date_details close_date_detail
      ON close_date_detail.date_actual = sfdc_opportunity_snapshot_history.close_date::DATE
    INNER JOIN date_details snapshot_date
      ON sfdc_opportunity_snapshot_history.date_actual::DATE = snapshot_date.date_actual
    LEFT JOIN date_details created_date_detail
      ON created_date_detail.date_actual = sfdc_opportunity_snapshot_history.created_date::DATE
    LEFT JOIN date_details iacv_created_date
      ON iacv_created_date.date_actual = sfdc_opportunity_snapshot_history.iacv_created_date::DATE

), sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  

), sfdc_users_xf AS (

    SELECT * 
    FROM {{ref('wk_sales_sfdc_users_xf')}}  

), sales_admin_hierarchy AS (
    
    SELECT
      opportunity_id,
      owner_id,
      'CRO'                                              AS level_1,
      CASE account_owner_team_stamped
        WHEN 'APAC'                 THEN 'VP Ent'
        WHEN 'Commercial'           THEN 'VP Comm SMB'
        WHEN 'Commercial - MM'      THEN 'VP Comm MM'
        WHEN 'Commercial - SMB'     THEN 'VP Comm SMB'
        WHEN 'EMEA'                 THEN 'VP Ent'
        WHEN 'MM - APAC'            THEN 'VP Comm MM'
        WHEN 'MM - East'            THEN 'VP Comm MM'
        WHEN 'MM - EMEA'            THEN 'VP Comm MM'
        WHEN 'MM - West'            THEN 'VP Comm MM'
        WHEN 'MM-EMEA'              THEN 'VP Comm MM'
        WHEN 'Public Sector'        THEN 'VP Ent'
        WHEN 'SMB'                  THEN 'VP Comm SMB'
        WHEN 'SMB - International'  THEN 'VP Comm SMB'
        WHEN 'SMB - US'             THEN 'VP Comm SMB'
        WHEN 'US East'              THEN 'VP Ent'
        WHEN 'US West'              THEN 'VP Ent'
        ELSE NULL
      END                                                AS level_2,
      CASE account_owner_team_stamped
        WHEN 'APAC'                 THEN 'RD APAC'
        WHEN 'EMEA'                 THEN 'RD EMEA'
        WHEN 'MM - APAC'            THEN 'ASM - MM - APAC'
        WHEN 'MM - East'            THEN 'ASM - MM - East'
        WHEN 'MM - EMEA'            THEN 'ASM - MM - EMEA'
        WHEN 'MM - West'            THEN 'ASM - MM - West'
        WHEN 'MM-EMEA'              THEN 'ASM - MM - EMEA'
        WHEN 'Public Sector'        THEN 'RD PubSec'
        WHEN 'US East'              THEN 'RD US East'
        WHEN 'US West'              THEN 'RD US West'
        ELSE NULL
      END                                                AS level_3
    FROM sfdc_opportunity_xf
    -- sfdc Sales Admin user
    WHERE owner_id = '00561000000mpHTAAY'

), net_iacv_to_net_arr_ratio AS (

    SELECT '2. New - Connected'       AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          1.001856868               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          0.9879780801              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          0.9999751852              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          0.9983306793              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          0.9427320642              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          0.9072734284              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          1.035889715               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'SMB'                     AS "USER_SEGMENT_STAMPED", 
          1                         AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'PubSec'                  AS "USER_SEGMENT_STAMPED", 
          1.002887983               AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '3. Growth'              AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          0.8504383811              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '1. New - First Order'   AS "ORDER_TYPE_STAMPED", 
          'Mid-Market'              AS "USER_SEGMENT_STAMPED", 
          0.9897881218              AS "RATIO_NET_IACV_TO_NET_ARR" 
    UNION 
    SELECT '2. New - Connected'     AS "ORDER_TYPE_STAMPED", 
          'Large'                   AS "USER_SEGMENT_STAMPED", 
          1.012723079               AS "RATIO_NET_IACV_TO_NET_ARR" 


), pipeline_type_quarter_start AS (

    SELECT 
      opportunity_id,
      snapshot_fiscal_quarter_date
    FROM sfdc_opportunity_snapshot_history        
    WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot
      -- not created within quarter
      AND snapshot_fiscal_quarter_date <> pipeline_created_fiscal_quarter_date
      -- set day 5 as start of the quarter for pipeline purposes
      AND snapshot_day_of_fiscal_quarter_normalised = 5
    GROUP BY 1, 2

), pipeline_type_quarter_created AS (

    SELECT 
      opportunity_id,
      snapshot_fiscal_quarter_date
    FROM sfdc_opportunity_snapshot_history
    WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot
      -- created same quarter
      AND snapshot_fiscal_quarter_date = pipeline_created_fiscal_quarter_date
    GROUP BY 1, 2

), sfdc_opportunity_snapshot_history_xf AS (

  SELECT DISTINCT

      opp_snapshot.*,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      -- Base helpers for reporting
      CASE 
        WHEN opp_snapshot.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                              ,'Developing', '1-Discovery', '2-Developing', '2-Scoping')  
          THEN 'Pipeline'
        WHEN opp_snapshot.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating'
                              , '6-Awaiting Signature', '7-Closing')                         
          THEN '3+ Pipeline'
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                       
          THEN 'Lost'
        WHEN opp_snapshot.stage_name IN ('Closed Won')                                                                                                         
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_3plus,
      
      CASE 
        WHEN opp_snapshot.stage_name IN ('00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
                            , 'Developing', '1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     
          THEN 'Pipeline'
        WHEN opp_snapshot.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                                               
          THEN '4+ Pipeline'
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                                                   
          THEN 'Lost'
        WHEN opp_snapshot.stage_name IN ('Closed Won')                                                                                                                                     
          THEN 'Closed Won'
        ELSE 'Other'
      END                                                         AS stage_name_4plus,


      CASE
        WHEN opp_snapshot.stage_name
          IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                         AS is_stage_1_plus,

      CASE 
        WHEN opp_snapshot.stage_name 
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1												                         
        ELSE 0
      END                                                         AS is_stage_3_plus,

      CASE 
        WHEN opp_snapshot.stage_name 
          IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1												                         
        ELSE 0
      END                                                         AS is_stage_4_plus,

      CASE 
        WHEN opp_snapshot.stage_name = 'Closed Won' 
          THEN 1 ELSE 0
      END                                                         AS is_won,

      CASE 
        WHEN opp_snapshot.stage_name = '8-Closed Lost'  
          THEN 1 ELSE 0
      END                                                         AS is_lost,

      CASE 
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
            THEN 0
        ELSE 1  
      END                                                         AS is_open,

      CASE 
        WHEN is_open = 0
          THEN 1
        ELSE 0
      END                                                         AS is_closed,
      
      CASE 
        WHEN is_won = 1  
          THEN '1.Won'
        WHEN is_lost = 1 
          THEN '2.Lost'
        WHEN is_open = 1 
          THEN '0. Open' 
        ELSE 'N/A'
      END                                                         AS stage_category,

      CASE 
        WHEN LOWER(opp_snapshot.sales_type) like '%renewal%' 
          THEN 1
        ELSE 0
      END                                                         AS is_renewal, 

  
      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
      
      -- Historical Net ARR Logic Summary   
      -- closed deals use net_incremental_acv
      -- open deals use incremental acv
      -- closed won deals with net_arr > 0 use that opportunity calculated ratio
      -- deals with no opty with net_arr use a default ratio for segment / order type
      -- deals before 2021-02-01 use always net_arr calculated from ratio
      -- deals after 2021-02-01 use net_arr if > 0, if open and not net_arr uses ratio version

      -- If the opportunity exists, use the ratio from the opportunity sheetload
      -- I am faking that using the opportunity table directly
      CASE 
        WHEN sfdc_opportunity_xf.is_won = 1 -- only consider won deals
          AND sfdc_opportunity_xf.opportunity_category <> 'Contract Reset' -- contract resets have a special way of calculating net iacv
          AND COALESCE(sfdc_opportunity_xf.raw_net_arr,0) <> 0
          AND COALESCE(sfdc_opportunity_xf.net_incremental_acv,0) <> 0
            THEN COALESCE(sfdc_opportunity_xf.raw_net_arr / sfdc_opportunity_xf.net_incremental_acv,0)
        ELSE NULL 
      END                                                                     AS opportunity_based_iacv_to_net_arr_ratio,
     
      -- If there is no opportnity, use a default table ratio
      -- I am faking that using the upper CTE, that should be replaced by the official table
      COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)         AS segment_order_type_iacv_to_net_arr_ratio,

      -- calculated net_arr
      -- uses ratios to estimate the net_arr based on iacv if open or net_iacv if closed
      -- if there is an opportunity based ratio, use that, if not, use default from segment / order type

      -- NUANCE: Lost deals might not have net_incremental_acv populated, so we must rely on iacv
      CASE 
        WHEN opp_snapshot.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
          THEN COALESCE(opp_snapshot.incremental_acv,0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio,segment_order_type_iacv_to_net_arr_ratio)
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
          AND COALESCE(opp_snapshot.net_incremental_acv,0) = 0
            THEN COALESCE(opp_snapshot.incremental_acv,0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio,segment_order_type_iacv_to_net_arr_ratio)
        WHEN opp_snapshot.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
            THEN COALESCE(opp_snapshot.net_incremental_acv,0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio,segment_order_type_iacv_to_net_arr_ratio)
        ELSE NULL
      END                                                                     AS calculated_from_ratio_net_arr,
      
      -- For opportunities before start of FY22, as Net ARR was WIP, there are a lot of opties with IACV or Net IACV and no Net ARR
      -- Those were later fixed in the opportunity object but stayed in the snapshot table.
      -- To account for those issues and give a directionally correct answer, we apply a ratio to everything before FY22
      CASE
        WHEN  opp_snapshot.snapshot_date < '2021-02-01'::DATE -- All deals before cutoff
          THEN calculated_from_ratio_net_arr
        WHEN  opp_snapshot.snapshot_date >= '2021-02-01'::DATE -- Open deal with no Net ARR, after cut off
          AND COALESCE(opp_snapshot.raw_net_arr,0) = 0
          AND opp_snapshot.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won') 
            THEN calculated_from_ratio_net_arr
        ELSE COALESCE(opp_snapshot.raw_net_arr,0) -- Rest of deals after cut off date
      END                                                                     AS net_arr,
         
      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------

      ---------------------
      -- compound metrics for reporting

      ------------------------------
      -- DEPRECATED IACV METRICS
      -- Use Net ARR instead
      CASE 
        WHEN opp_snapshot.created_fiscal_quarter_name= opp_snapshot.close_fiscal_quarter_name
          AND opp_snapshot.stage_name IN ('Closed Won')  
            THEN opp_snapshot.incremental_acv
        ELSE 0
      END                                                         AS created_and_won_same_quarter_iacv,

      -- created within quarter
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          THEN opp_snapshot.incremental_acv 
        ELSE 0 
      END                                                         AS created_in_snapshot_quarter_iacv,
      ------------------------------

      -- created and closed within the quarter net arr
      CASE 
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.close_fiscal_quarter_name
          AND opp_snapshot.stage_name IN ('Closed Won')  
            THEN net_arr
        ELSE 0
      END                                                         AS created_and_won_same_quarter_net_arr,

      -- created within quarter
      CASE
        WHEN opp_snapshot.pipeline_created_fiscal_quarter_name = opp_snapshot.snapshot_fiscal_quarter_name
          THEN net_arr
        ELSE 0 
      END                                                         AS created_in_snapshot_quarter_net_arr,

      -- booked net arr (won + renewals / lost)
      CASE
        WHEN opp_snapshot.stage_name = 'Closed Won'
          OR (opp_snapshot.stage_name = '8-Closed Lost'
            AND LOWER(opp_snapshot.sales_type) like '%renewal%')
          THEN net_arr
        ELSE 0 
      END                                                         AS booked_net_arr,


      -- fields for counting new logos, these fields count refund as negative
      CASE 
        WHEN opp_snapshot.is_refund = 1
          THEN -1
        ELSE 1
      END                                                         AS calculated_deal_count,

 
      -- opportunity driven fields
      sfdc_opportunity_xf.opportunity_owner_manager,
      sfdc_opportunity_xf.is_edu_oss,

      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
     
     -- field used for FY21 bookings reporitng
      sfdc_opportunity_xf.account_owner_team_stamped, 
     
      -- temporary, to deal with global reports that use account_owner_team_stamp field
      CASE 
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('Commercial - SMB','SMB','SMB - US','SMB - International')
          THEN 'SMB'
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('APAC','EMEA','Channel','US West','US East','Public Sector')
          THEN 'Large'
        WHEN sfdc_opportunity_xf.account_owner_team_stamped IN ('MM - APAC','MM - East','MM - EMEA','Commercial - MM','MM - West','MM-EMEA')
          THEN 'Mid-Market'
        ELSE 'SMB'
      END                                                         AS account_owner_team_stamped_cro_level,   

      -- Team Segment / ASM - RD 
      -- As the snapshot history table is used to compare current perspective with the past, I leverage the most recent version
      -- of the truth ato cut the data, that's why instead of using the stampped version, I take the current fields.
      sfdc_opportunity_xf.opportunity_owner_user_segment,
      sfdc_opportunity_xf.opportunity_owner_user_region,
      sfdc_opportunity_xf.opportunity_owner_cro_level,
      sfdc_opportunity_xf.opportunity_owner_rd_asm_level,

      /*CASE WHEN sfdc_opportunity_xf.user_segment_stamped IS NULL 
          THEN opportunity_owner.user_segment 
          ELSE COALESCE(sfdc_opportunity_xf.user_segment_stamped,'N/A')
      END                                                         AS opportunity_owner_user_segment,

      CASE WHEN sfdc_opportunity_xf.user_region_stamped IS NULL 
          THEN opportunity_owner.user_region
          ELSE COALESCE(sfdc_opportunity_xf.user_region_stamped,'N/A')
      END                                                         AS opportunity_owner_user_region,

      -- these two fields will be used to do cuts in X-Ray
      opportunity_owner_user_segment                                            AS opportunity_owner_cro_level,
      CONCAT(opportunity_owner_user_segment,'_',opportunity_owner_user_region)  AS opportunity_owner_rd_asm_level,
      
      */

      
      --------------------------------------------------------------------------------------------
      -- TO BE REMOVED
      -- account owner hierarchies levels
      COALESCE(account_owner.sales_team_level_2,'n/a')            AS account_owner_team_level_2,
      COALESCE(account_owner.sales_team_level_3,'n/a')            AS account_owner_team_level_3,
      COALESCE(account_owner.sales_team_level_4,'n/a')            AS account_owner_team_level_4,
      COALESCE(account_owner.sales_team_vp_level,'n/a')           AS account_owner_team_vp_level,
      COALESCE(account_owner.sales_team_rd_level,'n/a')           AS account_owner_team_rd_level,
      COALESCE(account_owner.sales_team_asm_level,'n/a')          AS account_owner_team_asm_level,
      COALESCE(account_owner.sales_min_hierarchy_level,'n/a')     AS account_owner_min_team_level,
      account_owner.sales_region                                  AS account_owner_sales_region,
  
      /*
      CASE 
          WHEN COALESCE(account_owner.sales_team_vp_level,'n/a') = 'VP Ent'
            THEN 'Large'
          WHEN COALESCE(account_owner.sales_team_vp_level,'n/a') = 'VP Comm MM'
            THEN 'Mid-Market'
          WHEN COALESCE(account_owner.sales_team_vp_level,'n/a') = 'VP Comm SMB' 
            THEN 'SMB' 
          ELSE 'Other' 
      END                                                         AS account_owner_cro_level,*/
  

      
      -- opportunity owner hierarchies levels
      CASE 
        WHEN sales_admin_hierarchy.level_2 IS NOT NULL 
          THEN sales_admin_hierarchy.level_2 
        ELSE opportunity_owner.sales_team_level_2
      END                                                         AS opportunity_owner_team_level_2,
      
      CASE 
        WHEN sales_admin_hierarchy.level_3 IS NOT NULL 
          THEN sales_admin_hierarchy.level_3 
        ELSE opportunity_owner.sales_team_level_3
      END                                                         AS opportunity_owner_team_level_3,    
      --------------------------------------------------------------------------------------------


      ------------------------------------------------------------------------------------------------------
      ------------------------------------------------------------------------------------------------------
     
      -- using current opportunity perspective instead of historical
      -- NF 2020-01-26: this might change to order type live 2.1     
      sfdc_opportunity_xf.order_type_stamped,     

      -- top level grouping of the order type field
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth', '5. Churn - Partial', '4. Churn','4. Contraction','6. Churn - Final') 
          THEN '2. Growth' 
        ELSE '3. Other'
      END                                                         AS deal_group,

      -- medium level grouping of the order type field
      CASE 
        WHEN sfdc_opportunity_xf.order_type_stamped = '1. New - First Order' 
          THEN '1. New'
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('2. New - Connected', '3. Growth') 
          THEN '2. Growth' 
        WHEN sfdc_opportunity_xf.order_type_stamped IN ('4. Churn','4. Contraction','5. Churn - Partial','6. Churn - Final')
          THEN '3. Churn'
        ELSE '4. Other' 
      END                                                         AS deal_category,
   
      -- account driven fields
      sfdc_accounts_xf.tsp_region,
      sfdc_accounts_xf.tsp_sub_region,
      sfdc_accounts_xf.ultimate_parent_sales_segment,
      sfdc_accounts_xf.tsp_max_hierarchy_sales_segment,
        
      -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
      CASE 
        WHEN sfdc_accounts_xf.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
          AND opp_snapshot.close_date < '2020-08-01' 
            THEN 1
        ELSE 0
      END                                                         AS is_excluded_flag,

      -- pipeline type, identifies if the opty was there at the begging of the quarter or not
      CASE
        WHEN pipeline_type_quarter_start.opportunity_id IS NOT NULL
          THEN '1. Starting Pipeline'
        WHEN pipeline_type_quarter_created.opportunity_id IS NOT NULL
          THEN '2. Created Pipeline'
        WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
          THEN '3. Pulled in Pipeline'
        ELSE '4. Not in Quarter'
      END                                                         AS pipeline_type

    FROM sfdc_opportunity_snapshot_history opp_snapshot
    INNER JOIN sfdc_opportunity_xf    
      ON sfdc_opportunity_xf.opportunity_id = opp_snapshot.opportunity_id
    LEFT JOIN sfdc_accounts_xf
      ON opp_snapshot.account_id = sfdc_accounts_xf.account_id 
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = sfdc_accounts_xf.owner_id
    LEFT JOIN sfdc_users_xf opportunity_owner
      ON opportunity_owner.user_id = opp_snapshot.owner_id
    LEFT JOIN sales_admin_hierarchy
      ON opp_snapshot.opportunity_id = sales_admin_hierarchy.opportunity_id
    -- Net IACV to Net ARR conversion table
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON net_iacv_to_net_arr_ratio.user_segment_stamped = sfdc_opportunity_xf.user_segment_stamped
      AND net_iacv_to_net_arr_ratio.order_type_stamped = sfdc_opportunity_xf.order_type_stamped
    -- Pipeline type - Starting pipeline
    LEFT JOIN pipeline_type_quarter_start 
      ON pipeline_type_quarter_start.opportunity_id = opp_snapshot.opportunity_id
      AND pipeline_type_quarter_start.snapshot_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date 
    -- Pipeline type - Created in Quarter
    LEFT JOIN pipeline_type_quarter_created 
      ON pipeline_type_quarter_created.opportunity_id = opp_snapshot.opportunity_id
      AND pipeline_type_quarter_created.snapshot_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date 
    WHERE opp_snapshot.account_id NOT IN ('0014M00001kGcORQA0')                           -- remove test account
      AND sfdc_accounts_xf.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')       -- remove test account
      AND opp_snapshot.is_deleted = 0
)

SELECT *
FROM sfdc_opportunity_snapshot_history_xf
  
 
