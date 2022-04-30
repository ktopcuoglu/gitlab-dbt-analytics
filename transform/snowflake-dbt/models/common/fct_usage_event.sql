{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('fct_usage_event_core', 'fct_usage_event_core')
    ])

}},

fct_usage_event AS (
    
    /*
    fct_usage_event is at the atomic grain of event_id and event_created_at timestamp. All other derived facts in the GitLab.com usage events 
    lineage are built from this derived fact. This CTE pulls in ALL of the columns from the fct_usage_event_core as a base data set. It uses the dbt_utils.star function 
    to select all columns except the meta data table related columns from the fact_usage_event_core. The CTE also filters out imported projects and events with 
    data quality issues by filtering out negative days since user creation at event date. It keeps events with a NULL days since user creation to capture events
    that do not have a user.
    */

    SELECT
      {{ dbt_utils.star(from=ref('fct_usage_event_core'), except=["CREATED_BY",
          "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
    FROM fct_usage_event_core
    WHERE days_since_user_creation_at_event_date >= 0
      OR days_since_user_creation_at_event_date IS NULL

)

{{ dbt_audit(
    cte_ref="fct_usage_event",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-04-09",
    updated_date="2022-04-09"
) }}
