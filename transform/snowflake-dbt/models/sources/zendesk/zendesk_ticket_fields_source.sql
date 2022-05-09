WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'ticket_fields') }}
    
), flattened AS (

    SELECT

      id                                    AS zendesk_ticket_field_id,
      active                                AS is_active,
      agent_description                     AS agent_description,
      collapsed_for_agents                  AS collapsed_for_agents,
      custom_field_options                  AS custom_field_options,
      description                           AS description,
      editable_in_portal                    AS editable_in_portal,
      position                              AS field_position,
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
      url                                   AS url,
      visible_in_portal                     AS visible_in_portal,
      updated_at                            AS updated_at,
      created_at                            AS created_at

    FROM source

)

SELECT *
FROM flattened
