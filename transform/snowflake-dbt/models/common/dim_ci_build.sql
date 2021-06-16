WITH prep_ci_build AS (

    SELECT *
    FROM {{ ref('prep_ci_build') }}

)

SELECT *
FROM prep_ci_build
