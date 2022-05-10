{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "table",
    "unique_key": "dim_user_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('source', 'gitlab_dotcom_users_source'),
    ('email_classification', 'driveload_email_domain_classification_source'),
    ('identity','gitlab_dotcom_identities_source')
]) }}

, email_classification_dedup AS (

    SELECT *
    FROM email_classification
    QUALIFY ROW_NUMBER() OVER(PARTITION BY domain ORDER BY domain DESC) = 1

)

, closest_provider AS (

    SELECT
        source.user_id                                                AS user_id,
        identity.identity_provider                                    AS identity_provider
    FROM 
        source                                                       
        LEFT JOIN identity 
        ON source.user_id = identity.user_id
    WHERE 
        identity.user_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY source.user_id 
                              ORDER BY TIMEDIFF(MILLISECONDS,source.created_at,COALESCE(identity.created_at,{{var('infinity_future')}})) ASC) = 1
)

, renamed AS (

    SELECT
      source.user_id                                                   AS dim_user_id,
      source.remember_created_at                                       AS remember_created_at,
      source.sign_in_count                                             AS sign_in_count,
      source.current_sign_in_at                                        AS current_sign_in_at,
      source.last_sign_in_at                                           AS last_sign_in_at,
      source.created_at                                                AS created_at,
      dim_date.date_id                                                 AS created_date_id,
      source.updated_at                                                AS updated_at,
      source.is_admin                                                  AS is_admin,
      source.state                                                     AS user_state,
      CASE 
        WHEN source.state in ('blocked', 'banned') THEN TRUE
        ELSE FALSE 
      END                                                              AS is_blocked_user,
      source.notification_email_domain                                 AS notification_email_domain,
      notification_email_domain.classification                         AS notification_email_domain_classification,
      source.email_domain                                              AS email_domain,
      email_domain.classification                                      AS email_domain_classification,
      source.public_email_domain                                       AS public_email_domain,
      public_email_domain.classification                               AS public_email_domain_classification,
      source.commit_email_domain                                       AS commit_email_domain,
      commit_email_domain.classification                               AS commit_email_domain_classification,
      closest_provider.identity_provider                               AS identity_provider 

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
    LEFT JOIN closest_provider AS closest_provider
      ON source.user_id = closest_provider.user_id  
    
)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet",
    updated_by="@tpoole",
    created_date="2021-05-31",
    updated_date="2022-05-05"
) }}
