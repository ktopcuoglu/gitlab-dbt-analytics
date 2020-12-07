WITH dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), dim_crm_opportunity AS (

    SELECT *
    FROM {{ ref('dim_crm_opportunity') }}

), dim_opportunity_source AS (

    SELECT *
    FROM {{ ref('dim_opportunity_source') }}

), dim_purchase_channel AS (

    SELECT *
    FROM {{ ref('dim_purchase_channel') }}

), fct_crm_opportunity AS (

    SELECT *
    FROM {{ ref('fct_crm_opportunity') }}

), final AS (

    SELECT
      fct_crm_opportunity.sales_accepted_date,
      fct_crm_opportunity.close_date,
      fct_crm_opportunity.created_date,
      fct_crm_opportunity.crm_opportunity_id,
      fct_crm_opportunity.is_won,
      fct_crm_opportunity.is_closed,
      fct_crm_opportunity.iacv,
      fct_crm_opportunity.days_in_sao,
      dim_crm_opportunity.is_edu_oss,
      dim_crm_opportunity.stage_name,
      dim_crm_opportunity.reason_for_loss,
      fct_crm_opportunity.is_sao,
      dim_purchase_channel.purchase_channel_name,
      dim_crm_opportunity.order_type,
      dim_opportunity_source.opportunity_source_name,
      dim_crm_account.ultimate_parent_gtm_strategy,
      dim_crm_account.ultimate_parent_account_segment,
      fct_crm_opportunity.closed_buckets
    FROM fct_crm_opportunity
    LEFT JOIN dim_crm_opportunity
      ON fct_crm_opportunity.crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN dim_crm_account
      ON dim_crm_opportunity.dim_crm_account_id = dim_crm_account.crm_account_id
    LEFT JOIN dim_opportunity_source
      ON fct_crm_opportunity.dim_opportunity_source_id = dim_opportunity_source.dim_opportunity_source_id
    LEFT JOIN dim_purchase_channel
      ON fct_crm_opportunity.dim_purchase_channel_id = dim_purchase_channel.dim_purchase_channel_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2020-12-07",
    updated_date="2020-12-07",
  ) }}
