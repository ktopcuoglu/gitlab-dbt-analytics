
{{ config({
    "materialized": "incremental",
    "unique_key": "group_id"
    })
}}


SELECT *
FROM {{ source('gitlab_dotcom', 'group_import_states') }}
{% if is_incremental() %}

WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY updated_at DESC) = 1
