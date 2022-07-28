WITH source AS (

    SELECT *
    FROM {{ ref('table_read_write_stats_source') }}

)

SELECT *
FROM source
