{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
        "materialized": "incremental",
        "primary_key": "event_primary_key",
        "automatic_clustering": true
    })
}}

/*
  Each dict must have ALL of the following:
    * event_name
    * primary_key
    * stage_name": "create",
    * "is_representative_of_stage
    * primary_key"
  Must have ONE of the following:
    * source_cte_name OR source_table_name
    * key_to_parent_project OR key_to_group_project (NOT both, see how clusters_applications_helm is included twice for group and project.
*/

{%- set event_ctes = [
  {
    "event_name": "ci_builds",
    "source_table_name": "temp_gitlab_dotcom_ci_builds_filtered",
    "user_column_name": "ci_build_user_id",
    "key_to_parent_project": "ci_build_project_id",
    "primary_key": "ci_build_id",
    "stage_name": "verify",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "events",
    "source_table_name": "gitlab_dotcom_events",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "event_id",
    "stage_name": "manage",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "push_events",
    "source_cte_name": "push_events_source",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "event_id",
    "stage_name": "create",
    "is_representative_of_stage": "False"
  },
]
-%}


{{ simple_cte([
    ('gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base'),
    ('namespaces', 'gitlab_dotcom_namespaces_xf'),
    ('plans', 'gitlab_dotcom_plans'),
    ('projects', 'gitlab_dotcom_projects_xf'),
    ('blocked_users', 'gitlab_dotcom_users_blocked_xf'),
    ('users', 'gitlab_dotcom_users'),
]) }}



{% for event_cte in event_ctes %}

, {{ event_cte.event_name }} AS (

    SELECT *,
      MD5({{ event_cte.primary_key }} || '-' || '{{ event_cte.event_name }}')   AS event_primary_key
    /* Check for source_table_name, else use source_cte_name. */
    {% if event_cte.source_table_name is defined %}
      FROM {{ ref(event_cte.source_table_name) }}
    {% else %}
      FROM {{ event_cte.source_cte_name }}
    {% endif %}
    WHERE created_at IS NOT NULL
      AND created_at >= DATEADD(MONTH, -25, CURRENT_DATE)
      
    {% if is_incremental() %}

      AND created_at >= (SELECT MAX(event_created_at) FROM {{this}} WHERE event_name = '{{ event_cte.event_name }}')

    {% endif %}

)

{% endfor -%}

, data AS (

{% for event_cte in event_ctes %}

    SELECT
      event_primary_key,
      '{{ event_cte.event_name }}'                                                                        AS event_name,
      ultimate_namespace.namespace_id,
      ultimate_namespace.namespace_created_at,
      IFF(blocked_users.user_id IS NOT NULL, TRUE, FALSE)                                                 AS is_blocked_namespace,
      {% if 'NULL' in event_cte.user_column_name %}
        NULL
      {% else %}
        {{ event_cte.event_name }}.{{ event_cte.user_column_name }}
      {% endif %}                                                                                         AS user_id,
      {% if event_cte.key_to_parent_project is defined %}
        'project'                                                                                         AS parent_type,
        projects.project_id                                                                               AS parent_id,
        projects.project_created_at                                                                       AS parent_created_at,
        projects.is_learn_gitlab                                                                          AS project_is_learn_gitlab,
      {% elif event_cte.key_to_parent_group is defined %}
        'group'                                                                                           AS parent_type,
        namespaces.namespace_id                                                                           AS parent_id,
        namespaces.namespace_created_at                                                                   AS parent_created_at,
        NULL                                                                                              AS project_is_learn_gitlab,
      {% else %}
        NULL                                                                                              AS parent_type,
        NULL                                                                                              AS parent_id,
        NULL                                                                                              AS parent_created_at,
        NULL                                                                                              AS project_is_learn_gitlab,
      {% endif %}
      ultimate_namespace.namespace_is_internal                                                            AS namespace_is_internal,
      {{ event_cte.event_name }}.created_at                                                               AS event_created_at,
      {{ event_cte.is_representative_of_stage }}::BOOLEAN                                                 AS is_representative_of_stage,
      '{{ event_cte.stage_name }}'                                                                        AS stage_name,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
      END                                                                                                 AS plan_id_at_event_date,
      CASE
        WHEN gitlab_subscriptions.is_trial
          THEN 'trial'
        ELSE COALESCE(plans.plan_name, 'free')
      END                                                                                                 AS plan_name_at_event_date,
      COALESCE(plans.plan_is_paid, FALSE)                                                                 AS plan_was_paid_at_event_date
    FROM {{ event_cte.event_name }}
      /* Join with parent project. */
      {% if event_cte.key_to_parent_project is defined %}
      LEFT JOIN projects
        ON {{ event_cte.event_name }}.{{ event_cte.key_to_parent_project }} = projects.project_id
      /* Join with parent group. */
      {% elif event_cte.key_to_parent_group is defined %}
      LEFT JOIN namespaces
        ON {{ event_cte.event_name }}.{{ event_cte.key_to_parent_group }} = namespaces.namespace_id
      {% endif %}

      -- Join on either the project's or the group's ultimate namespace.
      LEFT JOIN namespaces AS ultimate_namespace
        {% if event_cte.key_to_parent_project is defined %}
        ON ultimate_namespace.namespace_id = projects.ultimate_parent_id
        {% elif event_cte.key_to_parent_group is defined %}
        ON ultimate_namespace.namespace_id = namespaces.namespace_ultimate_parent_id
        {% else %}
        ON FALSE -- Don't join any rows.
        {% endif %}

      LEFT JOIN gitlab_subscriptions
        ON ultimate_namespace.namespace_id = gitlab_subscriptions.namespace_id
        AND {{ event_cte.event_name }}.created_at >= TO_DATE(gitlab_subscriptions.valid_from)
        AND {{ event_cte.event_name }}.created_at < {{ coalesce_to_infinity("TO_DATE(gitlab_subscriptions.valid_to)") }}
      LEFT JOIN plans
        ON gitlab_subscriptions.plan_id = plans.plan_id
      LEFT JOIN blocked_users
        ON ultimate_namespace.creator_id = blocked_users.user_id
    {% if 'NULL' not in event_cte.user_column_name %}
      WHERE {{ filter_out_blocked_users(event_cte.event_name , event_cte.user_column_name) }}
    {% endif %}


    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor -%}

)

, final AS (
    SELECT
      data.*,
      users.created_at                                    AS user_created_at,
      FLOOR(
      DATEDIFF('hour',
              namespace_created_at,
              event_created_at)/24)                       AS days_since_namespace_creation,
      FLOOR(
        DATEDIFF('hour',
                namespace_created_at,
                event_created_at)/(24 * 7))               AS weeks_since_namespace_creation,
      FLOOR(
        DATEDIFF('hour',
                parent_created_at,
                event_created_at)/24)                     AS days_since_parent_creation,
      FLOOR(
        DATEDIFF('hour',
                parent_created_at,
                event_created_at)/(24 * 7))               AS weeks_since_parent_creation,
      FLOOR(
        DATEDIFF('hour',
                user_created_at,
                event_created_at)/24)                     AS days_since_user_creation,
      FLOOR(
        DATEDIFF('hour',
                user_created_at,
                event_created_at)/(24 * 7))               AS weeks_since_user_creation
    FROM data
    LEFT JOIN users
      ON data.user_id = users.user_id
    WHERE event_created_at < CURRENT_DATE()

)

SELECT *
FROM final
