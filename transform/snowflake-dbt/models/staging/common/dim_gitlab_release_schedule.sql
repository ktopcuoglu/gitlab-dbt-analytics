WITH base AS (

    SELECT *
  FROM {{ ref('gitlab_release_schedule')}}

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@mpeychet",
    updated_by="@mpeychet",
    created_date="2020-11-23",
    updated_date="2020-11-23"
) }}
