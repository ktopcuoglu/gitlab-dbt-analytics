WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','engineering_contributing_organizations') }}

), renamed AS (

    SELECT
      "UUID"::VARCHAR                          AS uuid,
      "NAME"::VARCHAR                          AS name,
      "EMAIL"::VARCHAR                         AS email,
      "GITLAB_USER"::VARCHAR                   AS gitlab_user,
      "ORGANIZATION"                           AS organization
    FROM source

)

SELECT *
FROM renamed
