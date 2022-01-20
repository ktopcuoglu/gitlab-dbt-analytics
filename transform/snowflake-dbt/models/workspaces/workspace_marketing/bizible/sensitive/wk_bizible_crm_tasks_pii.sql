WITH source AS (

    SELECT *
    FROM {{ ref('bizible_crm_tasks_source') }}

)

SELECT *
FROM source