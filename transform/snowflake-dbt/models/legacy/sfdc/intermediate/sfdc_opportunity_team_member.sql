WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_team_member_source') }}

)

SELECT *
FROM base

