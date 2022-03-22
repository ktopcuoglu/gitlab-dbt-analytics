{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "alias": "gitlab_dotcom_ci_builds_dedupe_source",
    "post-hook": '{{ apply_dynamic_data_masking(columns = [{"id":"number"},{"commit_id":"number"},{"name":"string"},{"options":"string"},{"ref":"string"},{"user_id":"number"},{"project_id":"number"},{"erased_by_id":"number"},{"environment":"string"},{"yaml_variables":"string"},{"auto_canceled_by_id":"number"}]) }}'
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'ci_builds') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
