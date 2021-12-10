WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sales_capacity_kpi') }}

), renamed AS (

    SELECT
        month::DATE                                         AS month,
        NULLIF(target, 0)                                   AS target,
        NULLIF(actual, 0)                                   AS actual,      
        TO_TIMESTAMP(TO_NUMERIC("_UPDATED_AT"))::TIMESTAMP  AS last_updated_at
    FROM source

)

SELECT *
FROM renamed
