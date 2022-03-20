{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "alias": "gitlab_dotcom_notes_dedupe_source",
    "post-hook": '{{ apply_dynamic_data_masking(columns = [{"id":"number"},{"note":"string"},{"author_id":"float"},{"project_id":"number"},{"line_code":"string"},{"commit_id":"string"},{"noteable_id":"float"},{"updated_by_id":"float"},{"position":"string"},{"original_position":"string"},{"resolved_by_id":"string"},{"discussion_id":"string"},{"note_html":"string"},{"change_position":"string"}]) }}'
    })
}}



SELECT *
FROM {{ source('gitlab_dotcom', 'notes') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
