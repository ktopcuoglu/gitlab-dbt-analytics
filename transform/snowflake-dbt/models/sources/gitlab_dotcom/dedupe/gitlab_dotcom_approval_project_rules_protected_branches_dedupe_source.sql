
{{ config({
    "materialized": "incremental",
    "unique_key": "approval_project_rule_id"
    })
}}


SELECT *
FROM {{ source('gitlab_dotcom', 'approval_project_rules_protected_branches') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY approval_project_rule_id ORDER BY _uploaded_at DESC) = 1
