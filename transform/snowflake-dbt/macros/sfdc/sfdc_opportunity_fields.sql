{%- macro sfdc_opportunity_fields(model_type) %}

WITH first_contact  AS (

    SELECT
      opportunity_id,                                                             -- opportunity_id
      contact_id                                                                  AS sfdc_contact_id,
      md5(cast(coalesce(cast(contact_id as varchar), '') as varchar))             AS dim_crm_person_id,
      ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY created_date ASC)   AS row_num
    FROM {{ ref('sfdc_opportunity_contact_role_source')}}

), attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('sfdc_bizible_attribution_touchpoint_source') }}
    WHERE is_deleted = 'FALSE'

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), linear_attribution_base AS ( --the number of attribution touches a given opp has in total
    --linear attribution IACV of an opp / all touches (count_touches) for each opp - weighted by the number of touches in the given bucket (campaign,channel,etc)
    SELECT
     opportunity_id                                         AS dim_crm_opportunity_id,
     COUNT(DISTINCT attribution_touchpoints.touchpoint_id)  AS count_crm_attribution_touchpoints
    FROM  attribution_touchpoints
    GROUP BY 1

), campaigns_per_opp as (

    SELECT
      opportunity_id                                        AS dim_crm_opportunity_id,
      COUNT(DISTINCT attribution_touchpoints.campaign_id)   AS count_campaigns
    FROM attribution_touchpoints
    GROUP BY 1

), sfdc_opportunity_stage AS (

    SELECT *
    FROM {{ref('sfdc_opportunity_stage_source')}}

{%- if model_type == 'snapshot' %}
), snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2019-10-01'::DATE
      AND date_actual <= CURRENT_DATE  

), net_iacv_to_net_arr_ratio AS (

  SELECT
    '2. New - Connected' AS order_type,
    'Mid-Market' AS user_segment_stamped,
    0.999691784 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '1. New - First Order' AS order_type,
    'SMB' AS user_segment_stamped,
    0.998590143 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '1. New - First Order' AS order_type,
    'Large' AS user_segment_stamped,
    0.992289340 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '3. Growth' AS order_type,
    'SMB' AS user_segment_stamped,
    0.927846192 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '3. Growth' AS order_type,
    'Large' AS user_segment_stamped,
    0.852915435 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '2. New - Connected' AS order_type,
    'SMB' AS user_segment_stamped,
    1.009262672 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '3. Growth' AS order_type,
    'Mid-Market' AS user_segment_stamped,
    0.793618079 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '1. New - First Order' AS order_type,
    'Mid-Market' AS user_segment_stamped,
    0.988527875 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '2. New - Connected' AS order_type,
    'Large' AS user_segment_stamped,
    1.010081083 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '1. New - First Order' AS order_type,
    'PubSec' AS user_segment_stamped,
    1.000000000 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '2. New - Connected' AS order_type,
    'PubSec' AS user_segment_stamped,
    1.002741689 AS ratio_net_iacv_to_net_arr

  UNION

  SELECT
    '3. Growth' AS order_type,
    'PubSec' AS user_segment_stamped,
    0.965670500 AS ratio_net_iacv_to_net_arr

{%- endif %}

), live_opportunity_owner_fields AS (

  SELECT 
    sfdc_opportunity_source.opportunity_id AS dim_crm_opportunity_id,
    CASE 
      WHEN sfdc_opportunity_source.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
        THEN 0
      ELSE 1  
    END                                                                   AS is_open,
    CASE 
      WHEN sfdc_opportunity_source.user_segment_stamped IS NULL 
        OR is_open = 1
        THEN sfdc_users_source.user_segment 
      ELSE sfdc_opportunity_source.user_segment_stamped
    END                                                                   AS opportunity_owner_user_segment,

    CASE 
      WHEN sfdc_opportunity_source.user_geo_stamped IS NULL 
          OR is_open = 1
        THEN sfdc_users_source.user_geo
      ELSE sfdc_opportunity_source.user_geo_stamped
    END                                                                   AS opportunity_owner_user_geo,

      CASE 
        WHEN sfdc_opportunity_source.user_region_stamped IS NULL
             OR is_open = 1
          THEN sfdc_users_source.user_region
          ELSE sfdc_opportunity_source.user_region_stamped
      END                                                                   AS opportunity_owner_user_region,

      CASE
        WHEN sfdc_opportunity_source.user_area_stamped IS NULL
             OR is_open = 1
          THEN sfdc_users_source.user_area
        ELSE sfdc_opportunity_source.user_area_stamped
      END                                                                   AS opportunity_owner_user_area
  FROM {{ ref('sfdc_opportunity_source') }}
  INNER JOIN {{ ref('sfdc_users_source') }}
    ON sfdc_opportunity_source.owner_id = sfdc_users_source.user_id

), sfdc_opportunity AS (

    SELECT 
      account_id                                                         AS dim_crm_account_id,
      opportunity_id                                                     AS dim_crm_opportunity_id,
      owner_id                                                           AS dim_crm_user_id,
      order_type_stamped                                                 AS order_type,
      opportunity_term                                                   AS opportunity_term_base,
      {{ sales_qualified_source_cleaning('sales_qualified_source') }}    AS sales_qualified_source,
      user_segment_stamped                                               AS crm_opp_owner_sales_segment_stamped,
      user_geo_stamped                                                   AS crm_opp_owner_geo_stamped,
      user_region_stamped                                                AS crm_opp_owner_region_stamped,
      user_area_stamped                                                  AS crm_opp_owner_area_stamped,
      user_segment_geo_region_area_stamped                               AS crm_opp_owner_sales_segment_geo_region_area_stamped,
      created_date::DATE                                                 AS created_date,
      sales_accepted_date::DATE                                          AS sales_accepted_date,
      close_date::DATE                                                   AS close_date,
      net_arr                                                            AS raw_net_arr,
    {%- if model_type == 'live' %}
        {{ dbt_utils.star(from=ref('sfdc_opportunity_source'), except=["ACCOUNT_ID", "OPPORTUNITY_ID", "OWNER_ID", "ORDER_TYPE_STAMPED", "IS_WON", "ORDER_TYPE", "OPPORTUNITY_TERM","SALES_QUALIFIED_SOURCE", "DBT_UPDATED_AT", "CREATED_DATE", "SALES_ACCEPTED_DATE", "CLOSE_DATE", "NET_ARR"])}}
    {%- elif model_type == 'snapshot' %}
        {{ dbt_utils.surrogate_key(['sfdc_opportunity_snapshots_source.opportunity_id','snapshot_dates.date_id'])}}   AS crm_opportunity_snapshot_id,
        snapshot_dates.date_id                                                                                        AS snapshot_id,
        snapshot_dates.date_actual                                                                                    AS snapshot_date,
        snapshot_dates.first_day_of_month                                                                             AS snapshot_month,
        snapshot_dates.fiscal_year                                                                                    AS snapshot_fiscal_year,
        snapshot_dates.fiscal_quarter_name_fy                                                                         AS snapshot_fiscal_quarter_name,
        snapshot_dates.first_day_of_fiscal_quarter                                                                    AS snapshot_fiscal_quarter_date,
        snapshot_dates.day_of_fiscal_quarter_normalised                                                               AS snapshot_day_of_fiscal_quarter_normalised,
        snapshot_dates.day_of_fiscal_year_normalised                                                                  AS snapshot_day_of_fiscal_year_normalised,
        {{ dbt_utils.star(from=ref('sfdc_opportunity_snapshots_source'), except=["ACCOUNT_ID", "OPPORTUNITY_ID", "OWNER_ID", "ORDER_TYPE_STAMPED", "IS_WON", "ORDER_TYPE", "OPPORTUNITY_TERM", "SALES_QUALIFIED_SOURCE", "DBT_UPDATED_AT", "CREATED_DATE", "SALES_ACCEPTED_DATE", "CLOSE_DATE", "NET_ARR"])}}
     {%- endif %}
    FROM 
    {%- if model_type == 'live' %}
       {{ref('sfdc_opportunity_source')}}
    {%- elif model_type == 'snapshot' %}
        {{ ref('sfdc_opportunity_snapshots_source') }}
         INNER JOIN snapshot_dates
           ON sfdc_opportunity_snapshots_source.dbt_valid_from::DATE <= snapshot_dates.date_actual
           AND (sfdc_opportunity_snapshots_source.dbt_valid_to::DATE > snapshot_dates.date_actual OR sfdc_opportunity_snapshots_source.dbt_valid_to IS NULL)
    {%- endif %}
    WHERE account_id IS NOT NULL
      AND is_deleted = FALSE

), sfdc_zqu_quote_source AS (

    SELECT *
    FROM {{ ref('sfdc_zqu_quote_source') }}
    WHERE is_deleted = FALSE

), quote AS (

    SELECT DISTINCT
      sfdc_zqu_quote_source.zqu__opportunity                AS dim_crm_opportunity_id,
      sfdc_zqu_quote_source.quote_id                        AS dim_quote_id,
      sfdc_zqu_quote_source.zqu__start_date::DATE           AS quote_start_date,
      (ROW_NUMBER() OVER (PARTITION BY sfdc_zqu_quote_source.zqu__opportunity ORDER BY sfdc_zqu_quote_source.created_date DESC))
                                                            AS record_number
    FROM sfdc_zqu_quote_source
    INNER JOIN sfdc_opportunity
      ON sfdc_zqu_quote_source.zqu__opportunity = sfdc_opportunity.dim_crm_opportunity_id
    WHERE stage_name IN ('Closed Won', '8-Closed Lost')
      AND zqu__primary = TRUE
    QUALIFY record_number = 1

), sfdc_account AS (

    SELECT 
    {%- if model_type == 'live' %}
        *
    {%- elif model_type == 'snapshot' %}
        {{ dbt_utils.surrogate_key(['sfdc_account_snapshots_source.account_id','snapshot_dates.date_id'])}}   AS crm_account_snapshot_id,
        snapshot_dates.date_id                                                                                AS snapshot_id,
        sfdc_account_snapshots_source.*
     {%- endif %}
    FROM 
    {%- if model_type == 'live' %}
        {{ ref('sfdc_account_source') }}
    {%- elif model_type == 'snapshot' %}
        {{ ref('sfdc_account_snapshots_source') }}
          INNER JOIN snapshot_dates
            ON sfdc_account_snapshots_source.dbt_valid_from::DATE <= snapshot_dates.date_actual
            AND (sfdc_account_snapshots_source.dbt_valid_to::DATE > snapshot_dates.date_actual OR sfdc_account_snapshots_source.dbt_valid_to IS NULL)
    {%- endif %}
    WHERE account_id IS NOT NULL

), sfdc_user AS (

    SELECT 
    {%- if model_type == 'live' %}
        *
    {%- elif model_type == 'snapshot' %}
        {{ dbt_utils.surrogate_key(['sfdc_user_snapshots_source.user_id','snapshot_dates.date_id'])}}         AS crm_user_snapshot_id,
        snapshot_dates.date_id                                                                                AS snapshot_id,
        sfdc_user_snapshots_source.*
     {%- endif %}
    FROM 
    {%- if model_type == 'live' %}
        {{ ref('sfdc_users_source') }}
    {%- elif model_type == 'snapshot' %}
        {{ ref('sfdc_user_snapshots_source') }}
         INNER JOIN snapshot_dates
           ON sfdc_user_snapshots_source.dbt_valid_from::DATE <= snapshot_dates.date_actual
           AND (sfdc_user_snapshots_source.dbt_valid_to::DATE > snapshot_dates.date_actual OR sfdc_user_snapshots_source.dbt_valid_to IS NULL)
    {%- endif %}
    WHERE user_id IS NOT NULL

), final AS (

    SELECT
      -- opportunity information
      sfdc_opportunity.*,

      -- dates & date ids
      {{ get_date_id('sfdc_opportunity.created_date') }}                                          AS created_date_id,
      {{ get_date_id('sfdc_opportunity.sales_accepted_date') }}                                   AS sales_accepted_date_id,
      {{ get_date_id('sfdc_opportunity.close_date') }}                                            AS close_date_id,
      {{ get_date_id('sfdc_opportunity.stage_0_pending_acceptance_date') }}                       AS stage_0_pending_acceptance_date_id,
      {{ get_date_id('sfdc_opportunity.stage_1_discovery_date') }}                                AS stage_1_discovery_date_id,
      {{ get_date_id('sfdc_opportunity.stage_2_scoping_date') }}                                  AS stage_2_scoping_date_id,
      {{ get_date_id('sfdc_opportunity.stage_3_technical_evaluation_date') }}                     AS stage_3_technical_evaluation_date_id,
      {{ get_date_id('sfdc_opportunity.stage_4_proposal_date') }}                                 AS stage_4_proposal_date_id,
      {{ get_date_id('sfdc_opportunity.stage_5_negotiating_date') }}                              AS stage_5_negotiating_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_won_date') }}                               AS stage_6_closed_won_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_lost_date') }}                              AS stage_6_closed_lost_date_id,

      close_date_detail.first_day_of_month                                                        AS close_month,
      close_date_detail.fiscal_year                                                               AS close_fiscal_year,
      close_date_detail.fiscal_quarter_name_fy                                                    AS close_fiscal_quarter_name,
      close_date_detail.first_day_of_fiscal_quarter                                               AS close_fiscal_quarter_date,
      {%- if model_type == 'snapshot' %}
      90 - DATEDIFF(DAY, sfdc_opportunity.snapshot_date, close_date_detail.last_day_of_fiscal_quarter) AS close_day_of_fiscal_quarter_normalised,
      {%- endif %}

      created_date_detail.first_day_of_month                                                      AS created_month,
      created_date_detail.fiscal_year                                                             AS created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                                                  AS created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter                                             AS created_fiscal_quarter_date,

      net_arr_created_date.first_day_of_month                                                     AS iacv_created_month,
      net_arr_created_date.fiscal_year                                                            AS iacv_created_fiscal_year,
      net_arr_created_date.fiscal_quarter_name_fy                                                 AS iacv_created_fiscal_quarter_name,
      net_arr_created_date.first_day_of_fiscal_quarter                                            AS iacv_created_fiscal_quarter_date,

      created_date_detail.date_actual                                                             AS net_arr_created_date,
      created_date_detail.first_day_of_month                                                      AS net_arr_created_month,
      created_date_detail.fiscal_year                                                             AS net_arr_created_fiscal_year,
      created_date_detail.fiscal_quarter_name_fy                                                  AS net_arr_created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter                                             AS net_arr_created_fiscal_quarter_date,

      net_arr_created_date.date_actual                                                            AS pipeline_created_date,
      net_arr_created_date.first_day_of_month                                                     AS pipeline_created_month,
      net_arr_created_date.fiscal_year                                                            AS pipeline_created_fiscal_year,
      net_arr_created_date.fiscal_quarter_name_fy                                                 AS pipeline_created_fiscal_quarter_name,
      net_arr_created_date.first_day_of_fiscal_quarter                                            AS pipeline_created_fiscal_quarter_date,

      sales_accepted_date.first_day_of_month                                                      AS sales_accepted_month,
      sales_accepted_date.fiscal_year                                                             AS sales_accepted_fiscal_year,
      sales_accepted_date.fiscal_quarter_name_fy                                                  AS sales_accepted_fiscal_quarter_name,
      sales_accepted_date.first_day_of_fiscal_quarter                                             AS sales_accepted_fiscal_quarter_date,

      start_date.fiscal_quarter_name_fy                                                           AS subscription_start_date_fiscal_quarter_name,
      start_date.first_day_of_fiscal_quarter                                                      AS subscription_start_date_fiscal_quarter_date,
      start_date.fiscal_year                                                                      AS subscription_start_date_fiscal_year,
      start_date.first_day_of_month                                                               AS subscription_start_month,

      stage_1_date.first_day_of_month                                                             AS stage_1_discovery_month,
      stage_1_date.fiscal_year                                                                    AS stage_1_discovery_fiscal_year,
      stage_1_date.fiscal_quarter_name_fy                                                         AS stage_1_discovery_fiscal_quarter_name,
      stage_1_date.first_day_of_fiscal_quarter                                                    AS stage_1_discovery_fiscal_quarter_date,

      -- net arr
      {%- if model_type == 'live' %}
        raw_net_arr                                                                             AS net_arr,
      {%- elif model_type == 'snapshot' %}
      CASE 
        WHEN sfdc_opportunity_source_live.is_won = 1 -- only consider won deals
          AND sfdc_opportunity_source_live.opportunity_category <> 'Contract Reset' -- contract resets have a special way of calculating net iacv
          AND COALESCE(sfdc_opportunity_source_live.net_arr,0) <> 0
          AND COALESCE(sfdc_opportunity_source_live.net_incremental_acv,0) <> 0
            THEN COALESCE(sfdc_opportunity_source_live.net_arr / sfdc_opportunity_source_live.net_incremental_acv,0)
        ELSE NULL 
      END                                                                     AS opportunity_based_iacv_to_net_arr_ratio,
      -- If there is no opportunity, use a default table ratio
      -- I am faking that using the upper CTE, that should be replaced by the official table
      COALESCE(net_iacv_to_net_arr_ratio.ratio_net_iacv_to_net_arr,0)         AS segment_order_type_iacv_to_net_arr_ratio,
      -- calculated net_arr
      -- uses ratios to estimate the net_arr based on iacv if open or net_iacv if closed
      -- if there is an opportunity based ratio, use that, if not, use default from segment / order type
      -- NUANCE: Lost deals might not have net_incremental_acv populated, so we must rely on iacv
      -- Using opty ratio for open deals doesn't seem to work well
      CASE 
        WHEN sfdc_opportunity.stage_name NOT IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate')  -- OPEN DEAL
            THEN COALESCE(sfdc_opportunity.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost')                       -- CLOSED LOST DEAL and no Net IACV
          AND COALESCE(sfdc_opportunity.net_incremental_acv,0) = 0
            THEN COALESCE(sfdc_opportunity.incremental_acv,0) * COALESCE(segment_order_type_iacv_to_net_arr_ratio,0)
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Won')         -- REST of CLOSED DEAL
            THEN COALESCE(sfdc_opportunity.net_incremental_acv,0) * COALESCE(opportunity_based_iacv_to_net_arr_ratio,segment_order_type_iacv_to_net_arr_ratio)
        ELSE NULL
      END                                                                     AS calculated_from_ratio_net_arr,
      -- For opportunities before start of FY22, as Net ARR was WIP, there are a lot of opties with IACV or Net IACV and no Net ARR
      -- Those were later fixed in the opportunity object but stayed in the snapshot table.
      -- To account for those issues and give a directionally correct answer, we apply a ratio to everything before FY22
      CASE
        WHEN  sfdc_opportunity.snapshot_date < '2021-02-01'::DATE -- All deals before cutoff and that were not updated to Net ARR
          THEN calculated_from_ratio_net_arr
        WHEN  sfdc_opportunity.snapshot_date >= '2021-02-01'::DATE  -- After cutoff date, for all deals earlier than FY19 that are closed and have no net arr
              AND sfdc_opportunity.close_date < '2018-02-01'::DATE 
              AND sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
              AND COALESCE(sfdc_opportunity.raw_net_arr,0) = 0 
          THEN calculated_from_ratio_net_arr
        ELSE COALESCE(sfdc_opportunity.raw_net_arr,0) -- Rest of deals after cut off date
      END                                                                     AS net_arr,
     {%- endif %}
     
      -- opportunity flags
      CASE
        WHEN (sfdc_opportunity.days_in_stage > 30
          OR sfdc_opportunity.incremental_acv > 100000
          OR sfdc_opportunity.pushed_count > 0)
          THEN TRUE
          ELSE FALSE
      END                                                                                         AS is_risky,
      CASE
        WHEN sfdc_opportunity.opportunity_term_base IS NULL THEN
          DATEDIFF('month', quote.quote_start_date, sfdc_opportunity.subscription_end_date)
        ELSE sfdc_opportunity.opportunity_term_base
      END                                                                                         AS opportunity_term,
      -- opportunity stage information 
      sfdc_opportunity_stage.is_active                                                            AS stage_is_active,
      sfdc_opportunity_stage.is_closed                                                            AS stage_is_closed,
      sfdc_opportunity_stage.is_won                                                               AS is_won,
      CASE
        WHEN sfdc_opportunity.stage_name
          IN ('1-Discovery', '2-Developing', '2-Scoping','3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                                                         AS is_stage_1_plus,
      CASE 
        WHEN sfdc_opportunity.stage_name 
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1                                                 
        ELSE 0
      END                                                                                         AS is_stage_3_plus, 
      CASE 
        WHEN sfdc_opportunity.stage_name 
          IN ('4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')                               
            THEN 1                                                 
        ELSE 0
      END                                                                                         AS is_stage_4_plus,
      CASE 
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')  
          THEN 1 ELSE 0
      END                                                                                         AS is_lost,
      CASE 
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
            THEN 0
        ELSE 1  
      END                                                                                         AS is_open,
      CASE 
        WHEN LOWER(sfdc_opportunity.sales_type) like '%renewal%' 
          THEN 1
        ELSE 0
      END                                                                                         AS is_renewal,
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Decommission')
          THEN 1
        ELSE 0
      END                                                                                         AS is_decommissed,

     -- flags
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.stage_name != '10-Duplicate'
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_sao,
      CASE
        WHEN is_sao = TRUE
          AND sfdc_opportunity.sales_qualified_source IN (
                                        'SDR Generated'
                                        , 'BDR Generated'
                                        )
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_sdr_sao,
      CASE 
        WHEN (
               (sfdc_opportunity.sales_type = 'Renewal' AND sfdc_opportunity.stage_name = '8-Closed Lost')
                 OR sfdc_opportunity.stage_name = 'Closed Won'
              )
            AND sfdc_account.is_jihu_account = FALSE
          THEN TRUE 
        ELSE FALSE
      END                                                                                         AS is_net_arr_closed_deal,
      CASE
        WHEN sfdc_opportunity.new_logo_count = 1
          OR sfdc_opportunity.new_logo_count = -1
          THEN TRUE 
        ELSE FALSE
      END                                                                                         AS is_new_logo_first_order, 
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.stage_name NOT IN (
                                '00-Pre Opportunity'
                                , '10-Duplicate'
                                )
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_net_arr_pipeline_created,
      CASE
        WHEN sfdc_opportunity_stage.is_closed = TRUE
          AND sfdc_opportunity.amount >= 0
          AND (sfdc_opportunity.reason_for_loss IS NULL OR sfdc_opportunity.reason_for_loss != 'Merged into another opportunity')
          AND sfdc_opportunity.is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_win_rate_calc,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 'TRUE'
          AND sfdc_opportunity.is_closed = 'TRUE'
          AND sfdc_opportunity.is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_closed_won,
      CASE 
        WHEN lower(sfdc_opportunity.order_type_grouped) LIKE ANY ('%growth%', '%new%')
          AND sfdc_opportunity.is_edu_oss = 0
          AND is_stage_1_plus = 1
          AND sfdc_opportunity.forecast_category_name != 'Omitted'
          AND is_open = 1
         THEN 1
         ELSE 0
      END                                                                                         AS is_eligible_open_pipeline,
      CASE 
        WHEN sfdc_opportunity.order_type IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND sfdc_opportunity.is_edu_oss = 0
          AND pipeline_created_fiscal_quarter_date IS NOT NULL
          AND sfdc_opportunity.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')  
          AND (is_stage_1_plus = 1
            OR is_lost = 1)
          AND sfdc_opportunity.stage_name NOT IN ('10-Duplicate', '9-Unqualified')
          AND (net_arr > 0 
            OR sfdc_opportunity.opportunity_category = 'Credit')
          -- -- exclude vision opps from FY21-Q2
          -- AND (sfdc_opportunity.pipeline_created_fiscal_quarter_name != 'FY21-Q2'
          --       OR vision_opps.opportunity_id IS NULL)
          -- 20220128 Updated to remove webdirect SQS deals 
          AND sfdc_opportunity.sales_qualified_source  != 'Web Direct Generated'
              THEN 1
         ELSE 0
      END                                                                                         AS is_eligible_created_pipeline,
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
            THEN 1
        ELSE 0
      END                                                                                         AS is_eligible_sao,
      CASE 
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND sfdc_opportunity.order_type IN ('1. New - First Order','2. New - Connected','3. Growth')
          AND sfdc_opportunity.opportunity_category IN ('Standard','Ramp Deal','Internal Correction')
          AND ((sfdc_opportunity.is_web_portal_purchase = 1 
                AND net_arr > 0)
                OR sfdc_opportunity.is_web_portal_purchase = 0)
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_asp_analysis,
      CASE 
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND is_renewal = 0
          AND sfdc_opportunity.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
          AND sfdc_opportunity.opportunity_category IN ('Standard','Ramp Deal','Decommissioned')
          AND sfdc_opportunity.is_web_portal_purchase = 0
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_age_analysis,
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND (sfdc_opportunity_stage.is_won = 1 
              OR (is_renewal = 1 AND is_lost = 1))
          AND sfdc_opportunity.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_net_arr,
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.is_deleted = 0
          AND sfdc_opportunity.order_type IN ('4. Contraction','6. Churn - Final','5. Churn - Partial')
            THEN 1
          ELSE 0
      END                                                                                         AS is_eligible_churn_contraction,
      CASE 
        WHEN sfdc_opportunity.stage_name IN ('10-Duplicate')
            THEN 1
        ELSE 0
      END                                                                                         AS is_duplicate,
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Credit')
          THEN 1
        ELSE 0
      END                                                                                         AS is_credit,
      CASE
        WHEN sfdc_opportunity.opportunity_category IN ('Contract Reset')
          THEN 1
        ELSE 0
      END                                                                                         AS is_contract_reset,

      -- alliance type fields

      {{ alliance_partner_current('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type_current,
      {{ alliance_partner_short_current('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type_short_current,

      {{ alliance_partner('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.close_date', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type,
      {{ alliance_partner_short('fulfillment_partner.account_name', 'partner_account.account_name', 'sfdc_opportunity.close_date', 'sfdc_opportunity.partner_track', 'sfdc_opportunity.resale_partner_track', 'sfdc_opportunity.deal_path') }} AS alliance_type_short,

      --  quote information
      quote.dim_quote_id,
      quote.quote_start_date,

      -- contact information
      first_contact.dim_crm_person_id,
      first_contact.sfdc_contact_id,

      -- attribution information
      linear_attribution_base.count_crm_attribution_touchpoints,
      campaigns_per_opp.count_campaigns,
      sfdc_opportunity.incremental_acv/linear_attribution_base.count_crm_attribution_touchpoints   AS weighted_linear_iacv,

     -- opportunity attributes
      CASE
        WHEN sfdc_opportunity.days_in_sao < 0                  THEN '1. Closed in < 0 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 0 AND 30     THEN '2. Closed in 0-30 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 31 AND 60    THEN '3. Closed in 31-60 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 61 AND 90    THEN '4. Closed in 61-90 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 91 AND 180   THEN '5. Closed in 91-180 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 181 AND 270  THEN '6. Closed in 181-270 days'
        WHEN sfdc_opportunity.days_in_sao > 270                THEN '7. Closed in > 270 days'
        ELSE NULL
      END                                                                                         AS closed_buckets,
      CASE
        WHEN sfdc_opportunity.created_date < '2022-02-01' 
          THEN 'Legacy'
        WHEN sfdc_opportunity.opportunity_sales_development_representative IS NOT NULL AND sfdc_opportunity.opportunity_business_development_representative IS NOT NULL
          THEN 'SDR & BDR'
        WHEN sfdc_opportunity.opportunity_sales_development_representative IS NOT NULL
          THEN 'SDR'
        WHEN sfdc_opportunity.opportunity_business_development_representative IS NOT NULL
          THEN 'BDR'
        WHEN sfdc_opportunity.opportunity_business_development_representative IS NULL AND sfdc_opportunity.opportunity_sales_development_representative IS NULL
          THEN 'No XDR Assigned'
      END                                               AS sdr_or_bdr,
      CASE 
        WHEN sfdc_opportunity_stage.is_won = 1  
          THEN '1.Won'
        WHEN is_lost = 1 
          THEN '2.Lost'
        WHEN is_open = 1 
          THEN '0. Open' 
        ELSE 'N/A'
      END                                                                                         AS stage_category,
      CASE 
            WHEN sfdc_opportunity.order_type = '3. Growth' 
                THEN '2. Growth'
            WHEN sfdc_opportunity.order_type = '1. New - First Order' 
                THEN '1. New'
              ELSE '3. Other'
          END                                                                                    AS deal_group,
       CASE 
          WHEN sfdc_opportunity.order_type = '1. New - First Order' 
            THEN '1. New'
          WHEN sfdc_opportunity.order_type IN ('2. New - Connected', '3. Growth') 
            THEN '2. Growth' 
          WHEN sfdc_opportunity.order_type IN ('4. Contraction')
            THEN '3. Contraction'
          WHEN sfdc_opportunity.order_type IN ('5. Churn - Partial','6. Churn - Final')
            THEN '4. Churn'
          ELSE '5. Other' 
        END                                                                                       AS deal_category,
      COALESCE(sfdc_opportunity.reason_for_loss, sfdc_opportunity.downgrade_reason)               AS reason_for_loss_staged,
      CASE 
        WHEN reason_for_loss_staged IN ('Do Nothing','Other','Competitive Loss','Operational Silos') 
          OR reason_for_loss_staged IS NULL 
          THEN 'Unknown'
        WHEN reason_for_loss_staged IN ('Missing Feature','Product value/gaps','Product Value / Gaps',
                                          'Stayed with Community Edition','Budget/Value Unperceived') 
          THEN 'Product Value / Gaps'
        WHEN reason_for_loss_staged IN ('Lack of Engagement / Sponsor','Went Silent','Evangelist Left') 
          THEN 'Lack of Engagement / Sponsor'
        WHEN reason_for_loss_staged IN ('Loss of Budget','No budget') 
          THEN 'Loss of Budget'
        WHEN reason_for_loss_staged = 'Merged into another opportunity' 
          THEN 'Merged Opp'
        WHEN reason_for_loss_staged = 'Stale Opportunity' 
          THEN 'No Progression - Auto-close'
        WHEN reason_for_loss_staged IN ('Product Quality / Availability','Product quality/availability') 
          THEN 'Product Quality / Availability'
        ELSE reason_for_loss_staged
     END                                                                                        AS reason_for_loss_calc,
     CASE 
       WHEN sfdc_opportunity.order_type IN ('4. Contraction','5. Churn - Partial')
        THEN 'Contraction'
        ELSE 'Churn'
     END                                                                                        AS churn_contraction_type_calc,
     CASE 
        WHEN is_renewal = 1 
          AND subscription_start_date_fiscal_quarter_date >= close_fiscal_quarter_date 
         THEN 'On-Time'
        WHEN is_renewal = 1 
          AND subscription_start_date_fiscal_quarter_date < close_fiscal_quarter_date 
            THEN 'Late' 
      END                                                                                       AS renewal_timing_status,
      CASE 
        WHEN net_arr > -5000 
          THEN '1. < 5k'
        WHEN net_arr > -20000 AND net_arr <= -5000 
          THEN '2. 5k-20k'
        WHEN net_arr > -50000 AND net_arr <= -20000 
          THEN '3. 20k-50k'
        WHEN net_arr > -100000 AND net_arr <= -50000 
          THEN '4. 50k-100k'
        WHEN net_arr < -100000 
          THEN '5. 100k+'
      END                                                                                       AS churned_contraction_net_arr_bucket,
      CASE 
        WHEN sfdc_opportunity.deal_path = 'Direct'
          THEN 'Direct'
        WHEN sfdc_opportunity.deal_path = 'Web Direct'
          THEN 'Web Direct' 
        WHEN sfdc_opportunity.deal_path = 'Channel' 
            AND sfdc_opportunity.sales_qualified_source = 'Channel Generated' 
          THEN 'Partner Sourced'
        WHEN sfdc_opportunity.deal_path = 'Channel' 
            AND sfdc_opportunity.sales_qualified_source != 'Channel Generated' 
          THEN 'Partner Co-Sell'
      END                                                                                       AS deal_path_engagement,
      CASE 
        WHEN net_arr > 0 AND net_arr < 1000 
          THEN '1. (0k -1k)'
        WHEN net_arr >=1000 AND net_arr < 10000 
          THEN '2. (1k - 10k)'
        WHEN net_arr >=10000 AND net_arr < 50000 
          THEN '3. (10k - 50k)'
        WHEN net_arr >=50000 AND net_arr < 100000 
          THEN '4. (50k - 100k)'
        WHEN net_arr >= 100000 AND net_arr < 250000 
          THEN '5. (100k - 250k)'
        WHEN net_arr >= 250000 AND net_arr < 500000 
          THEN '6. (250k - 500k)'
        WHEN net_arr >= 500000 AND net_arr < 1000000 
          THEN '7. (500k-1000k)'
        WHEN net_arr >= 1000000 
          THEN '8. (>1000k)'
        ELSE 'Other' 
      END                                                                                         AS calculated_deal_size,
      CASE
        WHEN
          sfdc_opportunity.stage_name IN (
            '00-Pre Opportunity',
            '0-Pending Acceptance',
            '0-Qualifying',
            'Developing',
            '1-Discovery',
            '2-Developing',
            '2-Scoping'
          )
          THEN 'Pipeline'
        WHEN
          sfdc_opportunity.stage_name IN (
            '3-Technical Evaluation',
            '4-Proposal',
            '5-Negotiating',
            '6-Awaiting Signature',
            '7-Closing'
          )
          THEN '3+ Pipeline'
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END AS stage_name_3plus,
      CASE
        WHEN
          sfdc_opportunity.stage_name IN (
            '00-Pre Opportunity',
            '0-Pending Acceptance',
            '0-Qualifying',
            'Developing',
            '1-Discovery',
            '2-Developing',
            '2-Scoping',
            '3-Technical Evaluation'
          )
          THEN 'Pipeline'
        WHEN
          sfdc_opportunity.stage_name IN (
            '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing'
          )
          THEN '4+ Pipeline'
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
          THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
          THEN 'Closed Won'
        ELSE 'Other'
      END AS stage_name_4plus,

      -- counts and arr totals by pipeline stage
       CASE 
        WHEN is_decommissed = 1
          THEN -1
        WHEN is_credit = 1
          THEN 0
        ELSE 1
      END                                               AS calculated_deal_count,
      CASE 
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_1_plus = 1
            THEN calculated_deal_count  
        ELSE 0
      END                                               AS open_1plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_3_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_3plus_deal_count,

      CASE 
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_4_plus = 1
            THEN calculated_deal_count
        ELSE 0
      END                                               AS open_4plus_deal_count,
      CASE 
        WHEN sfdc_opportunity_stage.is_won = 1
          THEN calculated_deal_count
        ELSE 0
      END                                               AS booked_deal_count,
      CASE
        WHEN (
               (
                is_renewal = 1
                 AND is_lost = 1
               )
                OR sfdc_opportunity_stage.is_won = 1 
             )
            AND sfdc_opportunity.order_type IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN calculated_deal_count
        ELSE 0
      END                                               AS churned_contraction_deal_count,
      CASE 
        WHEN is_eligible_open_pipeline = 1
          THEN net_arr
        ELSE 0                                                                                              
      END                                                AS open_1plus_net_arr,

      CASE 
        WHEN is_eligible_open_pipeline = 1
          AND is_stage_3_plus = 1   
            THEN net_arr
        ELSE 0
      END                                                AS open_3plus_net_arr,
  
      CASE 
        WHEN is_eligible_open_pipeline = 1  
          AND is_stage_4_plus = 1
            THEN net_arr
        ELSE 0
      END                                                AS open_4plus_net_arr,
      CASE
        WHEN (
                sfdc_opportunity_stage.is_won = 1 
                OR (
                    is_renewal = 1 
                      AND is_lost = 1
                   )
             )
          THEN net_arr
        ELSE 0 
      END                                                 AS booked_net_arr,
      CASE
        WHEN (
               (
                 is_renewal = 1
                   AND is_lost = 1
                )
               OR sfdc_opportunity_stage.is_won = 1 
              )
            AND sfdc_opportunity.order_type IN ('5. Churn - Partial' ,'6. Churn - Final', '4. Contraction')
        THEN net_arr
        ELSE 0
      END                                                 AS churned_contraction_net_arr,
      {%- if model_type == 'snapshot' %}
      CASE
        WHEN is_open = 1
          THEN DATEDIFF(days, created_date_detail.date_actual, sfdc_opportunity.snapshot_date)
        WHEN is_open = 0 AND sfdc_opportunity.snapshot_date < close_date_detail.date_actual
          THEN DATEDIFF(days, created_date_detail.date_actual, sfdc_opportunity.snapshot_date)
        ELSE DATEDIFF(days, created_date_detail.date_actual, close_date_detail.date_actual)
      END                                                       AS calculated_age_in_days,
      CASE
        WHEN
        sfdc_account.ultimate_parent_account_id IN (
          '001610000111bA3',
          '0016100001F4xla',
          '0016100001CXGCs',
          '00161000015O9Yn',
          '0016100001b9Jsc'
        )
        AND sfdc_opportunity.close_date < '2020-08-01'
        THEN 1
      -- NF 2021 - Pubsec extreme deals
      WHEN
        sfdc_opportunity.dim_crm_opportunity_id IN ('0064M00000WtZKUQA3', '0064M00000Xb975QAB')
        AND sfdc_opportunity.snapshot_date < '2021-05-01'
        THEN 1
      -- exclude vision opps from FY21-Q2
      WHEN pipeline_created_fiscal_quarter_name = 'FY21-Q2'
        AND sfdc_opportunity.snapshot_day_of_fiscal_quarter_normalised = 90
        AND sfdc_opportunity.stage_name IN (
          '00-Pre Opportunity', '0-Pending Acceptance', '0-Qualifying'
        )
        THEN 1
      -- NF 20220415 PubSec duplicated deals on Pipe Gen -- Lockheed Martin GV - 40000 Ultimate Renewal
      WHEN
        sfdc_opportunity.dim_crm_opportunity_id IN (
          '0064M00000ZGpfQQAT', '0064M00000ZGpfVQAT', '0064M00000ZGpfGQAT'
        )
        THEN 1
       -- remove test accounts
       WHEN 
         sfdc_opportunity.dim_crm_account_id = '0014M00001kGcORQA0'
         THEN 1
       --remove test accounts
       WHEN (sfdc_account_source_live.ultimate_parent_account_id = ('0016100001YUkWVAA1')
            OR sfdc_account_source_live.account_id IS NULL) 
         THEN 1
       -- remove jihu accounts
       WHEN sfdc_account_source_live.is_jihu_account = 1 
         THEN 1
       -- remove deleted opps
        WHEN sfdc_opportunity.is_deleted = 1
          THEN 1
        ELSE 0
      END AS is_excluded,
      {%- endif %}
      live_opportunity_owner_fields.opportunity_owner_user_segment,
      live_opportunity_owner_fields.opportunity_owner_user_geo,
      live_opportunity_owner_fields.opportunity_owner_user_region,
      live_opportunity_owner_fields.opportunity_owner_user_area

    FROM sfdc_opportunity
    INNER JOIN sfdc_opportunity_stage
      ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
    LEFT JOIN quote
      ON sfdc_opportunity.dim_crm_opportunity_id = quote.dim_crm_opportunity_id
    LEFT JOIN linear_attribution_base
      ON sfdc_opportunity.dim_crm_opportunity_id = linear_attribution_base.dim_crm_opportunity_id
    LEFT JOIN campaigns_per_opp
      ON sfdc_opportunity.dim_crm_opportunity_id = campaigns_per_opp.dim_crm_opportunity_id
    LEFT JOIN first_contact
      ON sfdc_opportunity.dim_crm_opportunity_id = first_contact.opportunity_id AND first_contact.row_num = 1
    LEFT JOIN dim_date AS close_date_detail
      ON sfdc_opportunity.close_date = close_date_detail.date_actual
    LEFT JOIN dim_date AS created_date_detail
      ON sfdc_opportunity.created_date = created_date_detail.date_actual
    LEFT JOIN dim_date AS net_arr_created_date
      ON sfdc_opportunity.iacv_created_date::DATE = net_arr_created_date.date_actual 
    LEFT JOIN dim_date AS sales_accepted_date
      ON sfdc_opportunity.sales_accepted_date = sales_accepted_date.date_actual
    LEFT JOIN dim_date AS start_date
      ON sfdc_opportunity.subscription_start_date::DATE = start_date.date_actual
    LEFT JOIN dim_date AS stage_1_date
      ON sfdc_opportunity.stage_1_discovery_date::DATE = stage_1_date.date_actual
    LEFT JOIN sfdc_account AS fulfillment_partner
      ON sfdc_opportunity.fulfillment_partner = fulfillment_partner.account_id
    {%- if model_type == 'snapshot' %}
        AND sfdc_opportunity.snapshot_id = fulfillment_partner.snapshot_id
    {%- endif %}
    LEFT JOIN sfdc_account AS partner_account
      ON sfdc_opportunity.partner_account = partner_account.account_id
    {%- if model_type == 'snapshot' %}
        AND sfdc_opportunity.snapshot_id = partner_account.snapshot_id
    {%- endif %}
    LEFT JOIN sfdc_account
      ON sfdc_opportunity.dim_crm_account_id= sfdc_account.account_id
    {%- if model_type == 'snapshot' %}
        AND sfdc_opportunity.snapshot_id = sfdc_account.snapshot_id
    {%- endif %}
    LEFT JOIN sfdc_user
      ON sfdc_opportunity.dim_crm_user_id= sfdc_user.user_id
    {%- if model_type == 'snapshot' %}
        AND sfdc_opportunity.snapshot_id = sfdc_user.snapshot_id
    {%- endif %}
    LEFT JOIN live_opportunity_owner_fields
      ON sfdc_opportunity.dim_crm_opportunity_id = live_opportunity_owner_fields.dim_crm_opportunity_id
    {%- if model_type == 'snapshot' %}
    LEFT JOIN {{ ref('sfdc_opportunity_source') }} AS sfdc_opportunity_source_live
      ON sfdc_opportunity.dim_crm_opportunity_id = sfdc_opportunity_source_live.opportunity_id
    LEFT JOIN {{ ref('sfdc_account_source')}} AS sfdc_account_source_live
      ON sfdc_opportunity_source_live.account_id = sfdc_account_source_live.account_id
    LEFT JOIN net_iacv_to_net_arr_ratio
      ON live_opportunity_owner_fields.opportunity_owner_user_segment = net_iacv_to_net_arr_ratio.user_segment_stamped
      AND sfdc_opportunity_source_live.order_type_stamped = net_iacv_to_net_arr_ratio.order_type
    {%- endif %}

)

{%- endmacro %}