WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_lead_history_source') }}

)

SELECT *
FROM base