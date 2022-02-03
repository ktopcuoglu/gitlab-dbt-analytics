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

{%- if model_type == 'base' %}

{%- elif model_type == 'snapshot' %}
), snapshot_dates AS (

    SELECT *
    FROM {{ ref('dim_date') }}
    WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
    {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

   {% endif %}
{%- endif %}

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
    {%- if model_type == 'base' %}
        {{ dbt_utils.star(from=ref('sfdc_opportunity_source'), except=["ACCOUNT_ID", "OPPORTUNITY_ID", "OWNER_ID", "ORDER_TYPE_STAMPED", "IS_WON", "ORDER_TYPE", "OPPORTUNITY_TERM","SALES_QUALIFIED_SOURCE", "DBT_UPDATED_AT"])}}
    {%- elif model_type == 'snapshot' %}
        {{ dbt_utils.surrogate_key(['sfdc_opportunity_snapshots_source.opportunity_id','snapshot_dates.date_id'])}}   AS crm_opportunity_snapshot_id,
        snapshot_dates.date_id                                                                                        AS snapshot_id,
        {{ dbt_utils.star(from=ref('sfdc_opportunity_snapshots_source'), except=["ACCOUNT_ID", "OPPORTUNITY_ID", "OWNER_ID", "ORDER_TYPE_STAMPED", "IS_WON", "ORDER_TYPE", "OPPORTUNITY_TERM", "SALES_QUALIFIED_SOURCE", "DBT_UPDATED_AT"])}}
     {%- endif %}
    FROM 
    {%- if model_type == 'base' %}
       {{ref('sfdc_opportunity_source')}}
    {%- elif model_type == 'snapshot' %}
        {{ ref('sfdc_opportunity_snapshots_source') }}
         INNER JOIN snapshot_dates
           ON snapshot_dates.date_actual >= sfdc_opportunity_snapshots_source.dbt_valid_from
           AND snapshot_dates.date_actual < COALESCE(sfdc_opportunity_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
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
    {%- if model_type == 'base' %}
        *
    {%- elif model_type == 'snapshot' %}
        {{ dbt_utils.surrogate_key(['sfdc_account_snapshots_source.account_id','snapshot_dates.date_id'])}}   AS crm_account_snapshot_id,
        snapshot_dates.date_id                                                                                AS snapshot_id,
        sfdc_account_snapshots_source.*
     {%- endif %}
    FROM 
    {%- if model_type == 'base' %}
        {{ ref('sfdc_account_source') }}
    {%- elif model_type == 'snapshot' %}
        {{ ref('sfdc_account_snapshots_source') }}
         INNER JOIN snapshot_dates
           ON snapshot_dates.date_actual >= sfdc_account_snapshots_source.dbt_valid_from
           AND snapshot_dates.date_actual < COALESCE(sfdc_account_snapshots_source.dbt_valid_to, '9999-12-31'::TIMESTAMP)
    {%- endif %}
    WHERE account_id IS NOT NULL

), final AS (

    SELECT
      -- opportunity information
      sfdc_opportunity.*,
      CASE
        WHEN (sfdc_opportunity.days_in_stage > 30
          OR sfdc_opportunity.incremental_acv > 100000
          OR sfdc_opportunity.pushed_count > 0)
          THEN TRUE
          ELSE FALSE
      END                                                                                 AS is_risky,
      CASE
        WHEN sfdc_opportunity.opportunity_term_base IS NULL THEN
          DATEDIFF('month', quote.quote_start_date, sfdc_opportunity.subscription_end_date)
        ELSE sfdc_opportunity.opportunity_term_base
      END                                                                                 AS opportunity_term,
      -- opportunity stage information 
      sfdc_opportunity_stage.is_active                                                    AS stage_is_active,
      sfdc_opportunity_stage.is_closed                                                    AS stage_is_closed,
      sfdc_opportunity_stage.is_won                                                       AS is_won,

     -- flags
      CASE
        WHEN sfdc_opportunity.sales_accepted_date IS NOT NULL
          AND sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.stage_name != '10-Duplicate'
            THEN TRUE
        ELSE FALSE
      END                                                                               AS is_sao,
      CASE
        WHEN is_sao = TRUE
          AND sfdc_opportunity.sales_qualified_source IN (
                                        'SDR Generated'
                                        , 'BDR Generated'
                                        )
            THEN TRUE
        ELSE FALSE
      END                                                                             AS is_sdr_sao,
      sfdc_opportunity.fpa_master_bookings_flag                                       AS is_net_arr_closed_deal,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 'TRUE'
          AND sfdc_opportunity.is_closed = 'TRUE'
          AND sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.order_type = '1. New - First Order'
            THEN TRUE
        ELSE FALSE
      END                                                                             AS is_new_logo_first_order, 
      CASE
        WHEN sfdc_opportunity.is_edu_oss = 0
          AND sfdc_opportunity.stage_name NOT IN (
                                '00-Pre Opportunity'
                                , '10-Duplicate'
                                )
            THEN TRUE
        ELSE FALSE
      END                                                                             AS is_net_arr_pipeline_created,
      CASE
        WHEN sfdc_opportunity.stage_name IN ('Closed Won', '8-Closed Lost')
          AND sfdc_opportunity.amount >= 0
          AND (sfdc_opportunity.reason_for_loss IS NULL OR sfdc_opportunity.reason_for_loss != 'Merged into another opportunity')
          AND sfdc_opportunity.is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                             AS is_win_rate_calc,
      CASE
        WHEN sfdc_opportunity_stage.is_won = 'TRUE'
          AND sfdc_opportunity.is_closed = 'TRUE'
          AND sfdc_opportunity.is_edu_oss = 0
            THEN TRUE
        ELSE FALSE
      END                                                                             AS is_closed_won,
      CASE
        WHEN sfdc_opportunity.days_in_sao < 0                  THEN '1. Closed in < 0 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 0 AND 30     THEN '2. Closed in 0-30 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 31 AND 60    THEN '3. Closed in 31-60 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 61 AND 90    THEN '4. Closed in 61-90 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 91 AND 180   THEN '5. Closed in 91-180 days'
        WHEN sfdc_opportunity.days_in_sao BETWEEN 181 AND 270  THEN '6. Closed in 181-270 days'
        WHEN sfdc_opportunity.days_in_sao > 270                THEN '7. Closed in > 270 days'
        ELSE NULL
      END                                                                             AS closed_buckets,

      -- alliance type fields
      {{ alliance_type('fulfillment_partner.account_name', 'sfdc_opportunity.fulfillment_partner') }},
      {{ alliance_type_short('fulfillment_partner.account_name', 'sfdc_opportunity.fulfillment_partner') }},

      -- date ids
      {{ get_date_id('sfdc_opportunity.created_date') }}                              AS created_date_id,
      {{ get_date_id('sfdc_opportunity.sales_accepted_date') }}                       AS sales_accepted_date_id,
      {{ get_date_id('sfdc_opportunity.close_date') }}                                AS close_date_id,
      {{ get_date_id('sfdc_opportunity.stage_0_pending_acceptance_date') }}           AS stage_0_pending_acceptance_date_id,
      {{ get_date_id('sfdc_opportunity.stage_1_discovery_date') }}                    AS stage_1_discovery_date_id,
      {{ get_date_id('sfdc_opportunity.stage_2_scoping_date') }}                      AS stage_2_scoping_date_id,
      {{ get_date_id('sfdc_opportunity.stage_3_technical_evaluation_date') }}         AS stage_3_technical_evaluation_date_id,
      {{ get_date_id('sfdc_opportunity.stage_4_proposal_date') }}                     AS stage_4_proposal_date_id,
      {{ get_date_id('sfdc_opportunity.stage_5_negotiating_date') }}                  AS stage_5_negotiating_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_won_date') }}                   AS stage_6_closed_won_date_id,
      {{ get_date_id('sfdc_opportunity.stage_6_closed_lost_date') }}                  AS stage_6_closed_lost_date_id,

      --  quote information
      quote.quote_start_date,

      -- contact information
      first_contact.dim_crm_person_id,
      first_contact.sfdc_contact_id,

      -- attribution information
      linear_attribution_base.count_crm_attribution_touchpoints,
      campaigns_per_opp.count_campaigns,
      incremental_acv/linear_attribution_base.count_crm_attribution_touchpoints      AS weighted_linear_iacv

    FROM sfdc_opportunity
    INNER JOIN sfdc_opportunity_stage
      ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
    LEFT JOIN quote
      ON sfdc_opportunity.dim_crm_opportunity_id = quote.dim_crm_opportunity_id
    LEFT JOIN sfdc_account AS fulfillment_partner
      ON sfdc_opportunity.fulfillment_partner = fulfillment_partner.account_id
    LEFT JOIN linear_attribution_base
      ON sfdc_opportunity.dim_crm_opportunity_id = linear_attribution_base.dim_crm_opportunity_id
    LEFT JOIN campaigns_per_opp
      ON sfdc_opportunity.dim_crm_opportunity_id = campaigns_per_opp.dim_crm_opportunity_id
    LEFT JOIN first_contact
      ON sfdc_opportunity.dim_crm_opportunity_id = first_contact.opportunity_id AND first_contact.row_num = 1
    {%- if model_type == 'base' %}
    {%- elif model_type == 'snapshot' %}
        AND sfdc_opportunity.snapshot_id = fulfillment_partner.snapshot_id
    {%- endif %}

)

{%- endmacro %}