{{ config({
    "materialized": "incremental",
    "unique_key": "milestone_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'milestone_releases') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY milestone_id, release_id ORDER BY _uploaded_at DESC) = 1
