WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_split_source') }}

)

SELECT *
FROM base

