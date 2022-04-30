WITH prep_user AS (

    SELECT 
      dim_user_id,
      remember_created_at,
      sign_in_count,
      current_sign_in_at,
      last_sign_in_at,
      created_at,
      updated_at,
      is_admin,
      is_blocked_user,
      notification_email_domain,
      notification_email_domain_classification,
      email_domain,
      email_domain_classification,
      public_email_domain,
      public_email_domain_classification,
      commit_email_domain,
      commit_email_domain_classification
    FROM {{ ref('prep_user') }}

)

{{ dbt_audit(
    cte_ref="prep_user",
    created_by="@mpeychet_",
    updated_by="@jpeguero",
    created_date="2021-06-28",
    updated_date="2022-04-26"
) }}
