-- depends_on: {{ ref('engineering_productivity_metrics_projects_to_include') }}
-- depends_on: {{ ref('projects_part_of_product') }}

WITH merge_requests AS (

    SELECT 
      {{ dbt_utils.star(from=ref('gitlab_dotcom_merge_requests'), except=["created_at", "updated_at"]) }},
      created_at AS merge_request_created_at,
      updated_at  AS merge_request_updated_at
    FROM {{ref('gitlab_dotcom_merge_requests')}}

), label_states AS (

    SELECT
      label_id,
      merge_request_id
    FROM {{ref('gitlab_dotcom_label_states_xf')}}
    WHERE merge_request_id IS NOT NULL
      AND latest_state = 'added'

), all_labels AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_labels_xf')}}

), agg_labels AS (

    SELECT
      merge_requests.merge_request_id,
      ARRAY_AGG(LOWER(masked_label_title)) WITHIN GROUP (ORDER BY masked_label_title ASC) AS labels
    FROM merge_requests
    LEFT JOIN label_states
      ON merge_requests.merge_request_id = label_states.merge_request_id
    LEFT JOIN all_labels
      ON label_states.label_id = all_labels.label_id
    GROUP BY merge_requests.merge_request_id

),  latest_merge_request_metric AS (

    SELECT MAX(merge_request_metric_id) AS target_id
    FROM {{ref('gitlab_dotcom_merge_request_metrics')}}
    GROUP BY merge_request_id

),  merge_request_metrics AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_merge_request_metrics')}}
    INNER JOIN latest_merge_request_metric
    ON merge_request_metric_id = target_id

), milestones AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_milestones')}}

), projects AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_projects_xf')}}

), author_namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces_xf')}}
    WHERE namespace_type = 'Individual'

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}

), joined AS (

    SELECT
      merge_requests.*, 
      IFF(projects.visibility_level != 'public' AND projects.namespace_is_internal = FALSE,
        'content masked', milestones.milestone_title)         AS milestone_title,
      IFF(projects.visibility_level != 'public' AND projects.namespace_is_internal = FALSE,
        'content masked', milestones.milestone_description)   AS milestone_description,
      projects.namespace_id,
      projects.ultimate_parent_id,
      projects.ultimate_parent_plan_id,
      projects.ultimate_parent_plan_title,
      projects.ultimate_parent_plan_is_paid,
      projects.namespace_is_internal,
      author_namespaces.namespace_path                        AS author_namespace_path,
      ARRAY_TO_STRING(agg_labels.labels,'|')                  AS masked_label_title,
      agg_labels.labels,
      merge_request_metrics.merged_at,
      IFF(merge_requests.target_project_id IN ({{is_project_included_in_engineering_metrics()}}),
        TRUE, FALSE)                                          AS is_included_in_engineering_metrics,
      IFF(merge_requests.target_project_id IN ({{is_project_part_of_product()}}),
        TRUE, FALSE)                                          AS is_part_of_product,
      IFF(projects.namespace_is_internal IS NOT NULL
          AND ARRAY_CONTAINS('community contribution'::variant, agg_labels.labels),
        TRUE, FALSE)                                          AS is_community_contributor_related,
      TIMESTAMPDIFF(HOURS, merge_requests.merge_request_created_at,
        merge_request_metrics.merged_at)                      AS hours_to_merged_status,

      CASE 
        WHEN merge_request_created_at BETWEEN DATEADD('days', -30, gitlab_subscription_trial_ends_on) AND gitlab_subscription_trial_ends_on
          THEN 'trial'
        ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
      END AS namespace_plan_id_at_merge_request_creation

    FROM merge_requests
      LEFT JOIN agg_labels
        ON merge_requests.merge_request_id = agg_labels.merge_request_id
      LEFT JOIN merge_request_metrics
        ON merge_requests.merge_request_id = merge_request_metrics.merge_request_id
      LEFT JOIN milestones
        ON merge_requests.milestone_id = milestones.milestone_id
      LEFT JOIN projects
        ON merge_requests.target_project_id = projects.project_id
      LEFT JOIN author_namespaces
        ON merge_requests.author_id = author_namespaces.owner_id
      LEFT JOIN gitlab_subscriptions
        ON projects.ultimate_parent_id = gitlab_subscriptions.namespace_id
        AND merge_requests.created_at BETWEEN gitlab_subscriptions.valid_from AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}

)

SELECT *
FROM joined
