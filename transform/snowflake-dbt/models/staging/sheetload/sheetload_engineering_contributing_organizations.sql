WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_engineering_contributing_organizations_source') }}

), renamed AS (

    SELECT
      gitlab_user,
      organization
    FROM source
)

SELECT *
FROM renamed
