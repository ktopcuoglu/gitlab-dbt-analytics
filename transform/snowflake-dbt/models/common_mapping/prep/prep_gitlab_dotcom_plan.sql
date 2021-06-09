
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_plans_source') }}
  
), renamed AS (

    SELECT

      plan_id AS dim_plan_id,
      created_at,
      updated_at,
      plan_name,
      plan_title

    FROM source

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-05-30",
    updated_date="2021-05-30"
) }}
