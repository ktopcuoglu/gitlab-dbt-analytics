{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_epic_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('dim_namespace', 'dim_namespace'),
]) }}

, gitlab_dotcom_epics_dedupe_source AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_epics_dedupe_source') }} 
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), prep_user AS (
    
    SELECT *
    FROM {{ ref('prep_user') }} users
    WHERE {{ filter_out_blocked_users('users', 'dim_user_id') }}

), joined AS (

    SELECT 
      gitlab_dotcom_epics_dedupe_source.id::NUMBER                                           AS dim_epic_id,
      gitlab_dotcom_epics_dedupe_source.author_id::NUMBER                                    AS author_id,
      gitlab_dotcom_epics_dedupe_source.group_id::NUMBER AS group_id,
      prep_namespace.ultimate_parent_namespace_id::NUMBER                                    AS ultimate_parent_namespace_id,
      dim_date.date_id::NUMBER                                   AS creation_date_id,
      dim_namespace_plan_hist.dim_plan_id::NUMBER                                            AS dim_plan_id,
      gitlab_dotcom_epics_dedupe_source.assignee_id::NUMBER                                  AS assignee_id,
      gitlab_dotcom_epics_dedupe_source.iid::NUMBER                                          AS epic_internal_id,
      gitlab_dotcom_epics_dedupe_source.updated_by_id::NUMBER                                AS updated_by_id,
      gitlab_dotcom_epics_dedupe_source.last_edited_by_id::NUMBER                            AS last_edited_by_id,
      gitlab_dotcom_epics_dedupe_source.lock_version::NUMBER                                 AS lock_version,
      gitlab_dotcom_epics_dedupe_source.start_date::DATE                                     AS epic_start_date,
      gitlab_dotcom_epics_dedupe_source.end_date::DATE                                       AS epic_end_date,
      gitlab_dotcom_epics_dedupe_source.last_edited_at::TIMESTAMP                            AS epic_last_edited_at,
      gitlab_dotcom_epics_dedupe_source.created_at::TIMESTAMP                                AS created_at,
      gitlab_dotcom_epics_dedupe_source.updated_at::TIMESTAMP                                AS updated_at,
      gitlab_dotcom_epics_dedupe_source.title::VARCHAR                                       AS epic_title,
      gitlab_dotcom_epics_dedupe_source.description::VARCHAR                                 AS epic_description,
      gitlab_dotcom_epics_dedupe_source.closed_at::TIMESTAMP                                 AS closed_at,
      gitlab_dotcom_epics_dedupe_source.state_id::NUMBER                                     AS state_id,
      gitlab_dotcom_epics_dedupe_source.parent_id::NUMBER                                    AS parent_id,
      gitlab_dotcom_epics_dedupe_source.relative_position::NUMBER                            AS relative_position,
      gitlab_dotcom_epics_dedupe_source.start_date_sourcing_epic_id::NUMBER                  AS start_date_sourcing_epic_id,
      gitlab_dotcom_epics_dedupe_source.external_key::VARCHAR                                AS external_key,
      gitlab_dotcom_epics_dedupe_source.confidential::BOOLEAN                                AS is_confidential,
      {{ map_state_id('gitlab_dotcom_epics_dedupe_source.state_id') }}                       AS state,
      LENGTH(gitlab_dotcom_epics_dedupe_source.title)::NUMBER                                AS epic_title_length,
      LENGTH(gitlab_dotcom_epics_dedupe_source.description)::NUMBER                          AS epic_description_length
    FROM gitlab_dotcom_epics_dedupe_source
    LEFT JOIN dim_namespace 
        ON gitlab_dotcom_epics_dedupe_source.group_id = dim_namespace.dim_namespace_id
    LEFT JOIN dim_namespace_plan_hist 
        ON dim_namespace.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_epics_dedupe_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_epics_dedupe_source.created_at < dim_namespace_plan_hist.valid_to
    LEFT JOIN prep_user 
        ON gitlab_dotcom_epics_dedupe_source.author_id = prep_user.dim_user_id
    LEFT JOIN dim_date 
        ON TO_DATE(gitlab_dotcom_epics_dedupe_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-22",
    updated_date="2021-06-22"
) }}
