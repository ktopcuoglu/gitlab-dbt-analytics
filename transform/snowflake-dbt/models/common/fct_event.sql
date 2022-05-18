{{ config(
    tags=["mnpi_exception", "product"],
    materialized = "incremental",
    unique_key = "event_id"
) }}

{{ simple_cte([
    ('dim_date', 'dim_date')
    ])
}},

fct_events AS (

  SELECT
    prep_event_all.event_id,
    prep_event_all.event_name,
    prep_event_all.ultimate_parent_namespace_id,
    prep_event_all.dim_user_id,
    prep_event_all.parent_type,
    prep_event_all.parent_id,
    prep_event_all.dim_project_id,
    prep_event_all.event_created_at,
    prep_event_all.days_since_user_creation_at_event_date,
    prep_event_all.days_since_namespace_creation_at_event_date,
    prep_event_all.days_since_project_creation_at_event_date,
    CAST(prep_event_all.event_created_at AS DATE) AS event_date
  FROM {{ ref('prep_event_all') }}

),

paid_flag_by_day AS (

  SELECT
    ultimate_parent_namespace_id,
    plan_was_paid_at_event_date,
    plan_id_at_event_date,
    plan_name_at_event_date,
    event_created_at,
    CAST(event_created_at AS DATE) AS event_date
  FROM prep_event_all
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ultimate_parent_namespace_id, event_date
      ORDER BY event_created_at DESC) = 1

),

final AS (

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

gitlab_dotcom_fact AS (

  SELECT
    --Primary Key
    final.event_id,
    
    --Foreign Keys
    dim_date.date_id AS dim_event_date_id,
    final.ultimate_parent_namespace_id AS dim_ultimate_parent_namespace_id,
    final.dim_project_id,
    final.dim_user_id,
    
    --Time attributes
    final.event_created_at,
    final.event_date,
    
    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    final.parent_id,
    final.parent_type,
    final.event_name,
    final.plan_id_at_event_date,
    final.plan_name_at_event_date,
    final.plan_was_paid_at_event_date,
    final.days_since_user_creation_at_event_date,
    final.days_since_namespace_creation_at_event_date,
    final.days_since_project_creation_at_event_date,
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
    updated_date="2022-05-18"
) }}
