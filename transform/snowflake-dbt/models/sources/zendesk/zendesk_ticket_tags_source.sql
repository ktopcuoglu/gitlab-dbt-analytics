WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'tickets') }}

), renamed AS (

    -- Parse lists to values
    SELECT
      ID            AS ticket_id,
      f.VALUE       AS ticket_tag
    FROM
      source,
      table(FLATTEN(INPUT => STRTOK_TO_ARRAY(tags, '[], "'))) f

)

SELECT *
FROM renamed
