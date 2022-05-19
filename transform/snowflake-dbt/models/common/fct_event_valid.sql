{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_event', 'fct_event'),
    ('dim_user', 'dim_user')
    ])
}},

fct_event_valid AS (
    
    /*
    fct_event_valid is at the atomic grain of event_id and event_created_at timestamp. All other derived facts in the GitLab.com usage events 
    lineage are built from this derived fact. This CTE pulls in ALL of the columns from the fct_event as a base data set. It uses the dbt_utils.star function 
    to select all columns except the meta data table related columns from the fct_event. The CTE also filters out imported projects and events with 
    data quality issues by filtering out negative days since user creation at event date. It keeps events with a NULL days since user creation to capture events
    that do not have a user. fct_event also filters out events from blocked users with a join back to dim_user. The table also filters to a rolling 24 months of data 
    for performance optimization.
    */

    SELECT
      fct_event.dim_user_id,
      {{ dbt_utils.star(from=ref('fct_event'), except=["DIM_USER_ID", "CREATED_BY",
          "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
    FROM fct_event
    LEFT JOIN dim_user
      ON fct_event.dim_user_id = dim_user.dim_user_id
    WHERE DATE_TRUNC(MONTH,fct_event.event_created_at::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE)) 
      AND (fct_event.days_since_user_creation_at_event_date >= 0
           OR fct_event.days_since_user_creation_at_event_date IS NULL)
      AND (dim_user.is_blocked_user = FALSE 
           OR dim_user.is_blocked_user IS NULL)

)

{{ dbt_audit(
    cte_ref="fct_event_valid",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-05-16"
) }}
