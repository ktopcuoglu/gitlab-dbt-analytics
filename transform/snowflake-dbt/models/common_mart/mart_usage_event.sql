{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('fct_usage_event', 'fct_usage_event')
    ])
}},

fact_with_date AS (

  /*
  Mart Usage Event is at the atomic grain of event_id and event_created_at timestamp. All other Marts in the GitLab.com usage events 
  lineage are built from this Mart. This CTE pulls in ALL of the columns from the fct_usage_event as a base data set to join to the 
  dimensions. It uses the dbt_utils.star function to select all columns except the meta data table related columns from the fact.
  The CTE also filters out imported projects and events with data quality issues by filtering out negative days since user creation at
  event date.
  */
  SELECT
    {{ dbt_utils.star(from=ref('fct_usage_event'), except=["CREATED_BY",
        "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_usage_event
  WHERE days_since_user_creation_at_event_date >= 0
  
),

fact_with_namespace AS (

  SELECT
    fact_with_date.*,
    dim_namespace.namespace_is_internal,
    dim_namespace.created_at AS namespace_created_at,
    CAST(dim_namespace.created_at AS DATE) AS namespace_created_date,
    DATEDIFF(DAY, namespace_created_date, GETDATE()) AS days_since_namespace_created,
    DATEDIFF(DAY, namespace_created_date, event_date) AS days_since_namespace_creation_at_event_date
  FROM fact_with_date
  LEFT JOIN dim_namespace
    ON fact_with_date.dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id

)

{{ dbt_audit(
    cte_ref="fact_with_namespace",
    created_by="@dihle",
    updated_by="@iweeks",
    created_date="2022-01-28",
    updated_date="2022-04-09"
) }}
