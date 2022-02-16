WITH source AS (

    SELECT *
    FROM {{ source('zendesk_community_relations', 'organizations') }}

),

renamed AS (

    SELECT

      --ids
      id                                                  AS organization_id,

      --fields
      name                                                AS organization_name,
      tags                                                AS organization_tags,

      --dates
      created_at,
      updated_at

    FROM source

)

SELECT *
FROM renamed
