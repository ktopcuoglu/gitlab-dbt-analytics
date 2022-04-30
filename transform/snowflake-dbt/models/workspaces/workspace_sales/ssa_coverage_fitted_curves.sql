WITH base AS (

    SELECT *
    FROM {{ ref('driveload_ssa_coverage_fitted_curves_source') }}

)

SELECT *
FROM base
