WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_design_management_designs_source') }}

)

SELECT *
FROM source
