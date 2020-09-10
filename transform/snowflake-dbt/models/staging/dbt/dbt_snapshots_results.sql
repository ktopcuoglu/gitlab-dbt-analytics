WITH source AS (

    SELECT *
    FROM {{ ref('dbt_snapshots_results_source') }}

)

SELECT *
FROM source
