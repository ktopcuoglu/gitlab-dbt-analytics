{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
        "materialized": "incremental",
        "unique_key": "event_primary_key",
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
    "event_name": "incident_labeled_issues",
    "source_cte_name": "incident_labeled_issues_source",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "issue_id",
    "stage_name": "monitor",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "issues",
    "source_table_name": "gitlab_dotcom_issues",
    "user_column_name": "author_id",
    "key_to_parent_project": "project_id",
    "primary_key": "issue_id",
    "stage_name": "plan",
    "is_representative_of_stage": "True"
  },
  {
    "event_name": "issue_resource_label_events",
    "source_cte_name": "issue_resource_label_events_source",
    "user_column_name": "user_id",
    "key_to_parent_project": "namespace_id",
    "primary_key": "resource_label_event_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "issue_resource_weight_events",
    "source_table_name": "gitlab_dotcom_resource_weight_events_xf",
    "user_column_name": "user_id",
    "key_to_parent_project": "project_id",
    "primary_key": "resource_weight_event_id",
    "stage_name": "plan",
    "is_representative_of_stage": "False"
  },
  {
    "event_name": "issue_resource_milestone_events",
    "source_cte_name": "issue_resource_milestone_events_source",
    "user_column_name": "user_id",
    "key_to_parent_project": "project_id",
    "primary_key": "resource_milestone_event_id",
    "stage_name": "plan",
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
]
-%}


{{ simple_cte([
    ('gitlab_subscriptions', 'gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base'),
    ('namespaces', 'gitlab_dotcom_namespaces_xf'),
    ('plans', 'gitlab_dotcom_plans'),
    ('projects', 'gitlab_dotcom_projects_xf'),
    ('users', 'gitlab_dotcom_users'),
    ('blocked_users', 'gitlab_dotcom_users_blocked_xf')
]) }}


/* Source CTEs Start Here */
, incident_labeled_issues_source AS (

    SELECT
      *,
      issue_created_at AS created_at
    FROM {{ ref('gitlab_dotcom_issues_xf') }}
    WHERE ARRAY_CONTAINS('incident'::variant, labels)

), issue_resource_label_events_source AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_resource_label_events_xf')}}
    WHERE issue_id IS NOT NULL

), issue_resource_milestone_events_source AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_resource_milestone_events_xf')}}
    WHERE issue_id IS NOT NULL

)
/* End of Source CTEs */

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

      AND created_at > (SELECT MAX(event_created_at) FROM {{this}} WHERE event_name = '{{ event_cte.event_name }}')

    {% endif %}

)

{% endfor -%}

, data AS (

{% for event_cte in event_ctes %}

    SELECT 
      event_primary_key,
      '{{ event_cte.event_name }}' AS event_name,
      {{ event_cte.user_column_name }} AS user_id,
      created_at AS event_created_at,
      {{ event_cte.is_representative_of_stage }}::BOOLEAN AS is_representative_of_stage,
      '{{ event_cte.stage_name }}' AS stage_name,
      {% if event_cte.key_to_parent_project is defined -%}

      {{ event_cte.key_to_parent_project }} 
 
      {%- elif event_cte.key_to_parent_group is defined -%}

      {{ event_cte.key_to_parent_group }} 
      
      {%- else -%}
      NULL 
      {%- endif %}::NUMBER AS parent_id,
      {% if event_cte.key_to_parent_project is defined -%}
        'project'
       
      {%- elif event_cte.key_to_parent_group is defined -%}
        'group'
  
      {%- else -%}
        NULL     
      {%- endif %} AS parent_type
    FROM {{ event_cte.event_name }}                                                                              

    {% if not loop.last -%}
    UNION ALL
    {%- endif -%}
    {% endfor -%}

),

joins AS (
  SELECT
    data.event_primary_key,
    data.event_name,
    ultimate_namespace.namespace_id,
    ultimate_namespace.namespace_created_at,
    IFF(blocked_users.user_id IS NOT NULL, TRUE, FALSE) AS is_blocked_namespace,
    data.user_id,
    data.parent_type,
    data.parent_id,
    COALESCE(projects.project_created_at,namespaces.namespace_created_at) AS parent_created_at,
    projects.is_learn_gitlab AS project_is_learn_gitlab,
    ultimate_namespace.namespace_is_internal AS namespace_is_internal,
    data.event_created_at,
    data.is_representative_of_stage,
    data.stage_name,
    CASE
      WHEN gitlab_subscriptions.is_trial
        THEN 'trial'
      ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
    END AS plan_id_at_event_date,
    CASE
      WHEN gitlab_subscriptions.is_trial
        THEN 'trial'
      ELSE COALESCE(plans.plan_name, 'free')
    END AS plan_name_at_event_date,
    COALESCE(plans.plan_is_paid, FALSE) AS plan_was_paid_at_event_date
  FROM data
  /* Join with parent project. */

      LEFT JOIN projects
        ON data.parent_id = projects.project_id
        AND data.parent_type = 'project'
      /* Join with parent group. */
      LEFT JOIN namespaces
        ON data.parent_id = namespaces.namespace_id
        AND data.parent_type = 'group'

      -- Join on either the project's or the group's ultimate namespace.
      LEFT JOIN namespaces AS ultimate_namespace

        ON ultimate_namespace.namespace_id = COALESCE(projects.ultimate_parent_id,namespaces.namespace_ultimate_parent_id)


      LEFT JOIN gitlab_subscriptions
        ON ultimate_namespace.namespace_id = gitlab_subscriptions.namespace_id
        AND data.event_created_at >= TO_DATE(gitlab_subscriptions.valid_from)
        AND data.event_created_at < {{ coalesce_to_infinity("TO_DATE(gitlab_subscriptions.valid_to)") }}
      LEFT JOIN plans
        ON gitlab_subscriptions.plan_id = plans.plan_id
      LEFT JOIN blocked_users
        ON ultimate_namespace.creator_id = blocked_users.user_id 
      WHERE {{ filter_out_blocked_users('data' , 'user_id') }}
      


)
, final AS (
    SELECT
      joins.*,
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
    FROM joins
    LEFT JOIN users
      ON joins.user_id = users.user_id
    WHERE event_created_at < CURRENT_DATE()

)

SELECT *
FROM final
