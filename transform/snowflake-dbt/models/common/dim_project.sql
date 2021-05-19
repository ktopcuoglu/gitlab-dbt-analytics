WITH prep_project AS (

    SELECT *
    FROM {{ ref('prep_project') }}

), final AS (

  SELECT *
  FROM prep_project
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-05-19",
    updated_date="2021-05-19"
) }}
