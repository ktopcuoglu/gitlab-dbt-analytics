WITH mart_marketing_contact AS (
  
    SELECT
      {{ dbt_utils.star(from=ref('mart_marketing_contact'), except=['EMAIL_ADDRESS', 'FIRST_NAME', 'LAST_NAME', 'GITLAB_USER_NAME', 'GITLAB_DOTCOM_USER_ID',
      'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_UPDATED_AT', 'DBT_CREATED_AT']) }}
    FROM {{ ref('mart_marketing_contact') }}

)

{{ dbt_audit(
    cte_ref="mart_marketing_contact",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-05-13",
    updated_date="2021-05-13"
) }}
