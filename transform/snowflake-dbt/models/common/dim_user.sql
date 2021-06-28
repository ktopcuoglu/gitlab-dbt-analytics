WITH prep_user AS (

    SELECT 
      dim_user_id,
      remember_created_at,
      sign_in_count,
      current_sign_in_at,
      last_sign_in_at
      created_at,
      updated_at,
      is_admin
    FROM {{ ref('prep_user') }}

)

{{ dbt_audit(
    cte_ref="prep_user",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-28",
    updated_date="2021-06-28"
) }}
