WITH source AS (

    SELECT *
    FROM {{ ref('thanos_stage_group_error_budget_availability_source') }}

)
SELECT *
FROM source
