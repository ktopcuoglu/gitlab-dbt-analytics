WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_split_type_source') }}

)

SELECT *
FROM base

