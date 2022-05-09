WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'ticket_audits') }}
    
), flattened AS (

    SELECT

      active                                AS active,
      agent_description                     AS agent_description,
      collapsed_for_agents                  AS collapsed_for_agents,
      created_at                            AS created_at,
      custom_field_options                  AS custom_field_options,
      description                           AS description,
      editable_in_portal                    AS editable_in_portal,
      id                                    AS id,
      position                              AS position,
      raw_description                       AS raw_description,
      raw_title                             AS raw_title,
      raw_title_in_portal                   AS raw_title_in_portal,
      regexp_for_validation                 AS regexp_for_validation,
      removable                             AS removable,
      required                              AS required,
      required_in_portal                    AS required_in_portal,
      sub_type_id                           AS sub_type_id,
      system_field_options                  AS system_field_options,
      tag                                   AS tag,
      title                                 AS title,
      title_in_portal                       AS title_in_portal,
      type                                  AS type,
      updated_at                            AS updated_at,
      url                                   AS url,
      visible_in_portal                     AS visible_in_portal

    FROM source

)

SELECT *
FROM flattened
