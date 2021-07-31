WITH base AS (

    SELECT *
    FROM {{ ref('prep_crm_account') }}

)

SELECT *
FROM base
