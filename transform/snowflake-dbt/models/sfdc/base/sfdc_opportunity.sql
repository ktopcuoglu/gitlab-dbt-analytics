{{
  config(
    materialized = "table"
  )
}}

WITH source AS (

    SELECT *
    FROM {{ var("database") }}.salesforce_stitch.opportunity

),

    renamed AS (

      SELECT
        id                          AS opportunity_id,
        name                        AS opportunity_name,

        -- keys
        accountid                   AS account_id,
        ownerid                     AS owner_id,
        recordtypeid                AS record_type_id,

        -- logistical info

        opportunity_owner__c        AS owner,
        engagement_type__c          AS sales_path,
        sql_source__c               AS generated_source,
        COALESCE(
            initcap(
                COALESCE(sales_segmentation_employees_o__c, sales_segmentation_o__c)
                ), 'Unknown')       AS sales_segment,
        initcap(
            COALESCE(ultimate_parent_sales_segment_emp_o__c, ultimate_parent_sales_segment_o__c))
                                    AS parent_segment,
        type                        AS sales_type,
        closedate                   AS close_date,
        createddate                 AS created_date,
        stagename                   AS stage_name,
        products_purchased__c       AS product,
        datediff(days, current_date, (greatest(
            x0_pending_acceptance_date__c,
            x1_discovery_date__c,
            x2_scoping_date__c,
            x3_technical_evaluation_date__c,
            x4_proposal_date__c,
            x5_negotiating_date__c,
            --x6_closed_won_date__c,
            x7_closed_lost_date__c--,
            --x8_unqualified_date__c
        )::DATE)) + 1               AS days_in_stage,
        sales_accepted_date__c      AS sales_accepted_date,
        sales_qualified_date__c     AS sales_qualified_date,
        merged_opportunity__c       AS merged_opportunity_id,

        -- opp info
        acv_2__c                    AS acv,
        sql_source__c               AS sales_qualified_source,
        -- Should confirm which IACV is which
        incremental_acv_2__c        AS forecasted_iacv,
        incremental_acv__c          AS incremental_acv,
        renewal_amount__c           AS renewal_amount,
        renewal_acv__c              AS renewal_acv,
        nrv__c                      AS nrv,
        sales_segmentation_o__c     AS segment,
        amount                      AS total_contract_value,
        leadsource                  AS lead_source,
        products_purchased__c       AS products_purchased,
        competitors__C              AS competitors,
        solutions_to_be_replaced__c AS solutions_to_be_replaced,
        reason_for_lost__c          AS reason_for_loss,
        reason_for_lost_details__c  AS reason_for_loss_details,
        push_counter__c             AS pushed_count,
        upside_iacv__c              AS upside_iacv,
        upside_swing_deal_iacv__c   AS upside_swing_deal_iacv,
        --swing_deal__c                             as is_swing_deal,
        swing_deal__c               AS is_swing_deal,
        {{sfdc_deal_size('incremental_acv_2__c', 'deal_size')}},
        CASE WHEN acv_2__c >= 0
          THEN 1
        ELSE 0
        END                         AS closed_deals,
        -- so that you can exclude closed deals that had negative impact

        -- metadata
        isdeleted                   AS is_deleted,
        lastactivitydate            AS last_activity_date,
        convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run,
        datediff(days, lastactivitydate::date, CURRENT_DATE)
                                    AS days_since_last_activity


      FROM source
      WHERE accountid IS NOT NULL
            AND isdeleted = FALSE

  )

SELECT *
FROM renamed
