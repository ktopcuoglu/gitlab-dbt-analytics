{{ simple_cte([
    ('epic', 'gitlab_dotcom_epics_source'),
    ('map_namespace_internal', 'map_namespace_internal'),
    ('map_namespace_lineage', 'map_namespace_lineage')
]) }}

, epic_notes AS (

    SELECT
      noteable_id AS epic_id,
      *
    FROM {{ ref('gitlab_dotcom_notes_source') }}
    WHERE noteable_type = 'Epic'
      AND system = FALSE

), epic_extended AS (

    SELECT
      IFF(map_namespace_internal.ultimate_parent_namespace_id IS NULL, FALSE, TRUE) AS is_namespace_internal,
      map_namespace_lineage.dim_namespace_id_ultimate_parent,
      epic.*
    FROM epic
    INNER JOIN map_namespace_lineage
      ON epic.group_id = map_namespace_lineage.dim_namespace_id
    INNER JOIN map_namespace_internal
      ON map_namespace_lineage.dim_namespace_id_ultimate_parent = map_namespace_internal.ultimate_parent_namespace_id
    WHERE is_namespace_internal
    --  AND epic.epic_id = '2569'  -- Testing. TODO: Remove

),  gitlab_epic_description_parsing AS (

    SELECT
      epic_id,
      "{{this.database}}".{{target.schema}}.regexp_to_array(epic_description, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}') AS sfdc_link_array,
      "{{this.database}}".{{target.schema}}.regexp_to_array(epic_description, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}')      AS zendesk_link_array,
      SPLIT_PART(REGEXP_SUBSTR(epic_description, '~"customer priority::[0-9]{1,2}'), '::', -1)                                                        AS request_priority
    FROM epic_extended
    WHERE epic_description IS NOT NULL
      AND NOT (ARRAY_SIZE(sfdc_link_array) = 0 AND ARRAY_SIZE(zendesk_link_array) = 0)

), epic_notes_extended AS (

    SELECT epic_notes.*
    FROM epic_notes
    INNER JOIN epic_extended
      ON epic_notes.epic_id = epic_extended.epic_id

), gitlab_epic_notes_parsing AS (

    SELECT
      note_id,
      epic_id,
      "{{this.database}}".{{target.schema}}.regexp_to_array(note, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}') AS sfdc_link_array,
      "{{this.database}}".{{target.schema}}.regexp_to_array(note, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}')      AS zendesk_link_array,
      SPLIT_PART(REGEXP_SUBSTR(note, '~"customer priority::[0-9]{1,2}'), '::', -1)                                             AS request_priority,
      created_at                                                                                                               AS note_created_at,
      updated_at                                                                                                               AS note_updated_at
    FROM epic_notes_extended
    WHERE NOT (ARRAY_SIZE(sfdc_link_array) = 0 AND ARRAY_SIZE(zendesk_link_array) = 0)

), gitlab_epic_notes_sfdc_links AS (

    SELECT
      note_id,
      epic_id,
      "{{this.database}}".{{target.schema}}.id15to18(f.value::VARCHAR) AS sfdc_id_18char,
      SUBSTR(sfdc_id_18char, 0, 3) AS sfdc_id_prefix,
      CASE
        WHEN sfdc_id_prefix = '001' THEN 'Account'
        WHEN sfdc_id_prefix = '003' THEN 'Contact'
        WHEN sfdc_id_prefix = '00Q' THEN 'Lead'
        WHEN sfdc_id_prefix = '006' THEN 'Opportunity'
        ELSE NULL
      END                                               AS link_type,
      IFF(link_type = 'Account', sfdc_id_18char, NULL)  AS dim_crm_account_id,
      IFF(link_type = 'Opportunity', sfdc_id_18char, NULL)  AS dim_crm_opportunity_id,
      request_priority,
      note_created_at,
      note_updated_at
    FROM gitlab_epic_notes_parsing, 
      TABLE(FLATTEN(sfdc_link_array)) f

), gitlab_epic_description_sfdc_links AS (

    SELECT
      epic_id,
      "{{this.database}}".{{target.schema}}.id15to18(f.value::VARCHAR) AS sfdc_id_18char,
      SUBSTR(sfdc_id_18char, 0, 3) AS sfdc_id_prefix,
      CASE
        WHEN sfdc_id_prefix = '001' THEN 'Account'
        WHEN sfdc_id_prefix = '003' THEN 'Contact'
        WHEN sfdc_id_prefix = '00Q' THEN 'Lead'
        WHEN sfdc_id_prefix = '006' THEN 'Opportunity'
        ELSE NULL
      END                                            AS link_type,
      IFF(link_type = 'Account', sfdc_id_18char, NULL)  AS dim_crm_account_id,
      IFF(link_type = 'Opportunity', sfdc_id_18char, NULL)  AS dim_crm_opportunity_id,
      request_priority
    FROM gitlab_epic_description_parsing, 
    TABLE(FLATTEN(sfdc_link_array)) f

), gitlab_epic_notes_zendesk_link AS (

    SELECT
      note_id,
      epic_id,
      REPLACE(f.value, '"', '')                      AS dim_ticket_id,
      'Zendesk Ticket'                               AS link_type,
      request_priority,
      note_created_at,
      note_updated_at
    FROM gitlab_epic_notes_parsing, 
      TABLE(FLATTEN(zendesk_link_array)) f

), gitlab_epic_description_zendesk_link AS (

    SELECT
      epic_id,
      REPLACE(f.value, '"', '')                      AS dim_ticket_id,
      'Zendesk Ticket'                               AS link_type,
      request_priority
    FROM gitlab_epic_description_parsing, 
        TABLE(FLATTEN(zendesk_link_array)) f

), union_links AS (

    SELECT
      epic_id AS dim_epic_id,
      link_type,
      dim_crm_opportunity_id,
      dim_crm_account_id,
      NULL AS dim_ticket_id,
      request_priority
    FROM gitlab_epic_notes_sfdc_links
    QUALIFY ROW_NUMBER() OVER(PARTITION BY epic_id, sfdc_id_18char ORDER BY note_created_at DESC) = 1

    UNION

    SELECT
      epic_id,
      link_type,
      NULL dim_crm_opportunity_id,
      NULL dim_crm_account_id,
      dim_ticket_id,
      request_priority
    FROM gitlab_epic_notes_zendesk_link
    QUALIFY ROW_NUMBER() OVER(PARTITION BY epic_id, dim_ticket_id ORDER BY note_created_at DESC) = 1

    UNION

    SELECT
      gitlab_epic_description_sfdc_links.epic_id,
      gitlab_epic_description_sfdc_links.link_type,
      gitlab_epic_description_sfdc_links.dim_crm_opportunity_id,
      gitlab_epic_description_sfdc_links.dim_crm_account_id,
      NULL AS dim_ticket_id,
      gitlab_epic_description_sfdc_links.request_priority
    FROM gitlab_epic_description_sfdc_links
    LEFT JOIN gitlab_epic_notes_sfdc_links
      ON gitlab_epic_description_sfdc_links.epic_id = gitlab_epic_notes_sfdc_links.epic_id
      AND gitlab_epic_description_sfdc_links.sfdc_id_18char = gitlab_epic_notes_sfdc_links.sfdc_id_18char
    WHERE gitlab_epic_notes_sfdc_links.epic_id IS NULL

    UNION

    SELECT
      gitlab_epic_description_zendesk_link.epic_id,
      gitlab_epic_description_zendesk_link.link_type,
      NULL dim_crm_opportunity_id,
      NULL dim_crm_account_id,
      gitlab_epic_description_zendesk_link.dim_ticket_id,
      gitlab_epic_description_zendesk_link.request_priority
    FROM gitlab_epic_description_zendesk_link
    LEFT JOIN gitlab_epic_notes_zendesk_link
      ON gitlab_epic_description_zendesk_link.epic_id = gitlab_epic_notes_zendesk_link.epic_id
      AND gitlab_epic_description_zendesk_link.dim_ticket_id = gitlab_epic_notes_zendesk_link.dim_ticket_id
    WHERE gitlab_epic_notes_zendesk_link.epic_id IS NULL

)

SELECT *
FROM union_links
