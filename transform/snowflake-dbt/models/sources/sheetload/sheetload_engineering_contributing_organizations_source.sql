WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','engineering_contributing_organizations') }}

), renamed AS (

    SELECT
      uuid::VARCHAR                          AS uuid,
      name::VARCHAR                          AS name,
      email::VARCHAR                         AS email,
      gitlab_user::VARCHAR                   AS gitlab_user,
      organization::VARCHAR                  AS organization
    FROM source

)

SELECT *
FROM renamed
