{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ('dim_date', 'dim_date'),
    ('namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_namespace', 'dim_namespace')
    ])

}},

prep_event_24_months AS (

  SELECT *
  FROM {{ ref('prep_event_all') }}
  WHERE DATE_TRUNC(MONTH,event_created_at::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE))

),

fct_events AS (

  SELECT
    prep_event_24_months.event_id,
    prep_event_24_months.event_name,
    prep_event_24_months.ultimate_parent_namespace_id,
    'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' AS dim_instance_id,
    prep_event_24_months.dim_user_id,
    prep_event_24_months.parent_type,
    prep_event_24_months.parent_id,
    prep_event_24_months.dim_project_id,
    prep_event_24_months.event_created_at,
    prep_event_24_months.days_since_user_creation_at_event_date,
    xmau_metrics.group_name,
    xmau_metrics.section_name,
    xmau_metrics.smau AS is_smau,
    xmau_metrics.gmau AS is_gmau,
    xmau_metrics.is_umau,
    prep_event_24_months.project_is_learn_gitlab,
    COALESCE(prep_event_24_months.stage_name, xmau_metrics.stage_name) AS stage_name,
    CAST(prep_event_24_months.event_created_at AS DATE) AS event_date,
    DATE_TRUNC('MONTH', event_date) AS reporting_month,
    QUARTER(event_date) AS reporting_quarter,
    YEAR(event_date) AS reporting_year
  FROM prep_event_24_months
  LEFT JOIN xmau_metrics
    ON prep_event_24_months.event_name = xmau_metrics.events_to_include

),

paid_flag_by_day AS (

  SELECT
    ultimate_parent_namespace_id,
    plan_was_paid_at_event_date,
    plan_id_at_event_date,
    plan_name_at_event_date,
    event_created_at,
    CAST(event_created_at AS DATE) AS event_date
  FROM prep_event_24_months
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ultimate_parent_namespace_id, event_date
      ORDER BY event_created_at DESC) = 1

),

fct_events_w_plan_was_paid AS (

  SELECT
    fct_events.*,
    paid_flag_by_day.plan_was_paid_at_event_date,
    paid_flag_by_day.plan_id_at_event_date,
    paid_flag_by_day.plan_name_at_event_date
  FROM fct_events
  LEFT JOIN paid_flag_by_day
    ON fct_events.ultimate_parent_namespace_id = paid_flag_by_day.ultimate_parent_namespace_id
      AND CAST(fct_events.event_created_at AS DATE) = paid_flag_by_day.event_date

),

deduped_namespace_bdg AS (

  SELECT
    namespace_order_subscription.dim_subscription_id AS dim_active_subscription_id,
    namespace_order_subscription.order_id,
    namespace_order_subscription.dim_crm_account_id,
    namespace_order_subscription.dim_billing_account_id,
    namespace_order_subscription.dim_namespace_id
  FROM namespace_order_subscription
  INNER JOIN dim_subscription
    ON namespace_order_subscription.dim_subscription_id = dim_subscription.dim_subscription_id
  WHERE namespace_order_subscription.product_tier_name_subscription IN ('SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id ORDER BY subscription_version DESC) = 1

),

dim_namespace_w_bdg AS (

  SELECT
    dim_namespace.dim_namespace_id,
    dim_namespace.dim_product_tier_id AS dim_active_product_tier_id,
    deduped_namespace_bdg.dim_active_subscription_id,
    deduped_namespace_bdg.order_id,
    deduped_namespace_bdg.dim_crm_account_id,
    deduped_namespace_bdg.dim_billing_account_id
  FROM deduped_namespace_bdg
  INNER JOIN dim_namespace
    ON dim_namespace.dim_namespace_id = deduped_namespace_bdg.dim_namespace_id

),

final AS (

  SELECT 
    fct_events_w_plan_was_paid.*,
    dim_namespace_w_bdg.dim_active_product_tier_id,
    dim_namespace_w_bdg.dim_active_subscription_id,
    dim_namespace_w_bdg.order_id,
    dim_namespace_w_bdg.dim_crm_account_id,
    dim_namespace_w_bdg.dim_billing_account_id
  FROM fct_events_w_plan_was_paid
  LEFT JOIN dim_namespace_w_bdg
    ON fct_events_w_plan_was_paid.ultimate_parent_namespace_id = dim_namespace_w_bdg.dim_namespace_id

),

gitlab_dotcom_fact AS (

  SELECT
    --Primary Key
    final.event_id,
    
    --Foreign Keys
    final.dim_instance_id,
    final.dim_active_product_tier_id,
    final.dim_active_subscription_id,
    dim_date.date_id AS dim_event_date_id,
    final.dim_crm_account_id,
    final.dim_billing_account_id,
    final.ultimate_parent_namespace_id AS dim_ultimate_parent_namespace_id,
    final.dim_project_id,
    final.dim_user_id,
    
    --Time attributes
    final.event_created_at,
    final.event_date,
    final.reporting_month,
    final.reporting_quarter,
    final.reporting_year,
    
    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    final.parent_id,
    final.parent_type,
    final.is_smau,
    final.is_gmau,
    final.is_umau,
    final.event_name,
    final.stage_name,
    final.section_name,
    final.group_name,
    final.plan_id_at_event_date,
    final.plan_name_at_event_date,
    final.plan_was_paid_at_event_date,
    final.project_is_learn_gitlab,
    final.days_since_user_creation_at_event_date,
    'GITLAB_DOTCOM' AS data_source
  FROM final
  LEFT JOIN dim_date
    ON TO_DATE(event_created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="gitlab_dotcom_fact",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-01-20",
    updated_date="2022-04-09"
) }}
