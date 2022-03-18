WITH prep_user AS (

    SELECT *
    FROM {{ ref('prep_user') }}

)

{{ dbt_audit(
    cte_ref="prep_user",
    created_by="@mpeychet_",
    updated_by="@iweeks",
    created_date="2021-06-28",
    updated_date="2022-03-18"
) }}
