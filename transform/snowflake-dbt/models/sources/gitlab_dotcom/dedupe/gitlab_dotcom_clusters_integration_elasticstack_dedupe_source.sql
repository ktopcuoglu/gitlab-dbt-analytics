{{ config({
    "materialized": "incremental",
    "unique_key": "cluster_id"
    })
}}

SELECT *
FROM {{ source('gitlab_dotcom', 'clusters_integration_elasticstack') }}
{% if is_incremental() %}

WHERE _uploaded_at >= (SELECT MAX(_uploaded_at) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY _uploaded_at DESC) = 1
