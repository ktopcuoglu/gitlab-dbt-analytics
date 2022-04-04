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
]) }}

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
      END                                                              AS is_blocked_user
    FROM source
    LEFT JOIN dim_date
      ON TO_DATE(source.created_at) = dim_date.date_day
    
)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet",
    updated_by="@chrissharp",
    created_date="2021-05-31",
    updated_date="2022-03-23"
) }}
