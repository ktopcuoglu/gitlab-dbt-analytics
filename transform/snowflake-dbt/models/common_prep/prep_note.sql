{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_note_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
]) }}

, gitlab_dotcom_notes_dedupe_source AS (
    
    SELECT *
    FROM {{ ref('gitlab_dotcom_notes_dedupe_source') }} 
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), prep_user AS (
    
    SELECT *
    FROM {{ ref('prep_user') }} users
    WHERE {{ filter_out_blocked_users('users', 'dim_user_id') }}

), joined AS (

    SELECT 
      gitlab_dotcom_notes_dedupe_source.id::NUMBER                                            AS dim_note_id,
      gitlab_dotcom_notes_dedupe_source.author_id::NUMBER                                     AS author_id,
      gitlab_dotcom_notes_dedupe_source.project_id::NUMBER                                    AS dim_project_id,
      prep_project.ultimate_parent_namespace_id::NUMBER                                       AS ultimate_parent_namespace_id,
      gitlab_dotcom_notes_dedupe_source.noteable_id::NUMBER                                   AS noteable_id,
      dim_date.date_id::NUMBER                                                                AS created_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)::NUMBER                                 AS dim_plan_id,
      IFF(noteable_type = '', NULL, noteable_type)::VARCHAR                                   AS noteable_type,
      gitlab_dotcom_notes_dedupe_source.created_at::TIMESTAMP                                 AS created_at,
      gitlab_dotcom_notes_dedupe_source.updated_at::TIMESTAMP                                 AS updated_at,
      gitlab_dotcom_notes_dedupe_source.note::VARCHAR                                         AS note,
      gitlab_dotcom_notes_dedupe_source.attachment::VARCHAR                                   AS attachment,
      gitlab_dotcom_notes_dedupe_source.line_code::VARCHAR                                    AS line_code,
      gitlab_dotcom_notes_dedupe_source.commit_id::VARCHAR                                    AS commit_id,
      gitlab_dotcom_notes_dedupe_source.system::BOOLEAN                                       AS is_system_note,
      gitlab_dotcom_notes_dedupe_source.updated_by_id::NUMBER                                 AS note_updated_by_id,
      gitlab_dotcom_notes_dedupe_source.position::VARCHAR                                     AS position_number,
      gitlab_dotcom_notes_dedupe_source.original_position::VARCHAR                            AS original_position,
      gitlab_dotcom_notes_dedupe_source.resolved_at::TIMESTAMP                                AS resolved_at,
      gitlab_dotcom_notes_dedupe_source.resolved_by_id::NUMBER                                AS resolved_by_id,
      gitlab_dotcom_notes_dedupe_source.discussion_id::VARCHAR                                AS discussion_id,
      gitlab_dotcom_notes_dedupe_source.cached_markdown_version::NUMBER                       AS cached_markdown_version,
      gitlab_dotcom_notes_dedupe_source.resolved_by_push::BOOLEAN                             AS resolved_by_push
    FROM gitlab_dotcom_notes_dedupe_source
    LEFT JOIN prep_project ON gitlab_dotcom_notes_dedupe_source.project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_notes_dedupe_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_notes_dedupe_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN prep_user ON gitlab_dotcom_notes_dedupe_source.author_id = prep_user.dim_user_id
    LEFT JOIN dim_date ON TO_DATE(gitlab_dotcom_notes_dedupe_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mpeychet_",
    created_date="2021-06-22",
    updated_date="2021-06-22"
) }}
