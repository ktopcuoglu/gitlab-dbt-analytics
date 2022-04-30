{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('fct_usage_event_daily', 'fct_usage_event_daily'),
    ('dim_user', 'dim_user'),
    ('dim_project', 'dim_project'),
    ('dim_date', 'dim_date')
    ])
}},

fact_with_date AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_usage_event_daily'), except=["CREATED_BY",
        "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_usage_event_daily
  
),

fact_with_namespace AS (

  SELECT
    fact_with_date.*,
    dim_namespace.namespace_is_internal,
    dim_namespace.namespace_creator_is_blocked,
    dim_namespace.created_at AS namespace_created_at,
    CAST(dim_namespace.created_at AS DATE) AS namespace_created_date
  FROM fact_with_date
  LEFT JOIN dim_namespace
    ON fact_with_date.dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id

),

fact_with_user AS (
    
    SELECT
      fact_with_namespace.*,
      dim_user.created_at AS user_created_at
    FROM fact_with_namespace
    LEFT JOIN dim_user
      ON fact_with_namespace.dim_user_id = dim_user.dim_user_id
        
),

fact_with_project AS (
    
    SELECT
      fact_with_user.*,
      COALESCE(dim_project.is_learn_gitlab, FALSE) AS project_is_learn_gitlab,
      COALESCE(dim_project.is_imported, FALSE) AS project_is_imported
    FROM fact_with_user
    LEFT JOIN dim_project
      ON fact_with_user.dim_project_id = dim_project.dim_project_id
        
),

fact_with_dim_date AS (
    
    SELECT
      fact_with_project.*,
      dim_date.first_day_of_month AS event_calendar_month,
      dim_date.quarter_name AS event_calendar_quarter,
      dim_date.year_actual AS event_calendar_year
    FROM fact_with_project
    LEFT JOIN dim_date
      ON fact_with_project.dim_event_date_id = dim_date.date_id
        
)

{{ dbt_audit(
    cte_ref="fact_with_dim_date",
    created_by="@dihle",
    updated_by="@iweeks",
    created_date="2022-01-28",
    updated_date="2022-04-09"
) }}