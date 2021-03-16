WITH source AS (

    SELECT *
    FROM {{ source('marketing_dnc', 'dnc_list') }}

), renamed AS (

    SELECT
      email_address::VARCHAR         AS email_address,
      is_role::BOOLEAN               AS is_role,
      is_disposable::BOOLEAN         AS is_disposable,
      did_you_mean::VARCHAR          AS did_you_mean,
      result::VARCHAR                AS result,
      reason::VARCHAR                AS reason,
      risk::VARCHAR                  AS risk,
      root_email_address::VARCHAR    AS root_email_address
    FROM source

)

SELECT *
FROM renamed
