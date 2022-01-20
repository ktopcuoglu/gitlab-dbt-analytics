WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_crm_tasks_source') }}
    FROM {{ ref('bizible_crm_tasks_source') }}

)

SELECT *
FROM source