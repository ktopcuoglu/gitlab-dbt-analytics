{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_user_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('source', 'gitlab_dotcom_users_source'),
    ('email_classification', 'driveload_email_domain_classification_source')
]) }}

, email_classification_dedup AS (

    SELECT *
    FROM email_classification
    QUALIFY ROW_NUMBER() OVER(PARTITION BY domain ORDER BY domain DESC) = 1

)

, renamed AS (

    SELECT
      user_id                                                          AS dim_user_id,
      remember_created_at                                              AS remember_created_at,
      sign_in_count                                                    AS sign_in_count,
      current_sign_in_at                                               AS current_sign_in_at,
      last_sign_in_at                                                  AS last_sign_in_at,
      created_at                                                       AS created_at,
      dim_date.date_id                                                 AS created_date_id,
      updated_at                                                       AS updated_at,
      is_admin                                                         AS is_admin,
      state                                                            AS user_state,
      CASE 
        WHEN state in ('blocked', 'banned') THEN TRUE
        ELSE FALSE 
      END                                                              AS is_blocked_user,
      source.notification_email_domain                                 AS notification_email_domain,
      notification_email_domain.classification                         AS notification_email_domain_classification,
      source.email_domain                                              AS email_domain,
      email_domain.classification                                      AS email_domain_classification,
      source.public_email_domain                                       AS public_email_domain,
      public_email_domain.classification                               AS public_email_domain_classification,
      source.commit_email_domain                                       AS commit_email_domain,
      commit_email_domain.classification                               AS commit_email_domain_classification

    FROM source
    LEFT JOIN dim_date
      ON TO_DATE(source.created_at) = dim_date.date_day
    LEFT JOIN email_classification_dedup AS notification_email_domain
      ON notification_email_domain.domain = source.notification_email_domain
    LEFT JOIN email_classification_dedup AS email_domain
      ON email_domain.domain = source.email_domain
    LEFT JOIN email_classification_dedup AS public_email_domain
      ON public_email_domain.domain = source.public_email_domain
    LEFT JOIN email_classification_dedup AS commit_email_domain
      ON commit_email_domain.domain = source.commit_email_domain
    
)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet",
    updated_by="@jpeguero",
    created_date="2021-05-31",
    updated_date="2022-04-26"
) }}
