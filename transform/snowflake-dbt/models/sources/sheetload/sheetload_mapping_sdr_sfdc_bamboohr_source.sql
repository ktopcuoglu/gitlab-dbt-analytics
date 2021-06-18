WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'mapping_sdr_sfdc_bamboohr') }}

), renamed as (

    SELECT
      user_id::VARCHAR                      AS user_id,
      first_name::VARCHAR                   AS first_name,
      last_name::VARCHAR                    AS last_name,
      username::VARCHAR                     AS username,
      active::NUMBER                        AS active,
      profile::VARCHAR                      AS profile,
      eeid::NUMBER                          AS eeid
    FROM source

)

SELECT *
FROM renamed
