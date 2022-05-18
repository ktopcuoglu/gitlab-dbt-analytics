{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_event', 'fct_event'),
    ('dim_user', 'dim_user'),
    ('xmau_metrics', 'map_gitlab_dotcom_xmau_metrics'),
    ('namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_namespace', 'dim_namespace')
    ])
}},

fct_event_valid AS (
    
    /*
    fct_event_valid is at the atomic grain of event_id and event_created_at timestamp. All other derived facts in the GitLab.com usage events 
    lineage are built from this derived fact. This CTE pulls in ALL of the columns from the fct_event as a base data set. It uses the dbt_utils.star function 
    to select all columns except the meta data table related columns from the fct_event. The CTE also filters out imported projects and events with 
    data quality issues by filtering out negative days since user creation at event date. It keeps events with a NULL days since user creation to capture events
    that do not have a user. fct_event_valid also filters out events from blocked users with a join back to dim_user. The table also filters to a rolling 24 months of data 
    for performance optimization.
    */

    SELECT
      fct_event.dim_user_id,
      {{ dbt_utils.star(from=ref('fct_event'), except=["DIM_USER_ID", "CREATED_BY",
          "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }},
      xmau_metrics.group_name,
      xmau_metrics.section_name,
      xmau_metrics.stage_name,
      xmau_metrics.smau AS is_smau,
      xmau_metrics.gmau AS is_gmau,
      xmau_metrics.is_umau
    FROM fct_event
    LEFT JOIN xmau_metrics
      ON fct_event.event_name = xmau_metrics.common_events_to_include
    LEFT JOIN dim_user
      ON fct_event.dim_user_id = dim_user.dim_user_id
    WHERE DATE_TRUNC(MONTH,fct_event.event_created_at::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE)) 
      AND (fct_event.days_since_user_creation_at_event_date >= 0
           OR fct_event.days_since_user_creation_at_event_date IS NULL)
      AND (dim_user.is_blocked_user = FALSE 
           OR dim_user.is_blocked_user IS NULL)

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
    fct_event_valid.*,
    dim_namespace_w_bdg.dim_active_product_tier_id,
    dim_namespace_w_bdg.dim_active_subscription_id,
    dim_namespace_w_bdg.order_id,
    dim_namespace_w_bdg.dim_crm_account_id,
    dim_namespace_w_bdg.dim_billing_account_id
  FROM fct_event_valid
  LEFT JOIN dim_namespace_w_bdg
    ON fct_event_valid.dim_ultimate_parent_namespace_id = dim_namespace_w_bdg.dim_namespace_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-05-18"
) }}
