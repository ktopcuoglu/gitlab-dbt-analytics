{{ simple_cte([
    ('issue', 'gitlab_dotcom_issues_source'),
    ('map_namespace_internal', 'map_namespace_internal'),
    ('map_namespace_lineage', 'map_namespace_lineage'),
    ('project', 'gitlab_dotcom_projects_source'),
    ('zendesk_ticket', 'zendesk_tickets_source'),
    ('zendesk_organization', 'zendesk_organizations_source'),
    ('map_moved_issue', 'map_moved_issue')
]) }}

, issue_notes AS (

    SELECT
      noteable_id AS issue_id,
      *
    FROM {{ ref('gitlab_dotcom_notes_source') }}
    WHERE noteable_type = 'Issue'
      AND system = FALSE

), issue_extended AS (

    SELECT
      IFF(map_namespace_internal.ultimate_parent_namespace_id IS NULL, FALSE, TRUE) AS is_namespace_internal,
      map_namespace_lineage.dim_namespace_ultimate_parent_id,
      issue.*
    FROM issue
    INNER JOIN project
      ON project.project_id = issue.project_id
    INNER JOIN map_namespace_lineage
      ON project.namespace_id = map_namespace_lineage.dim_namespace_id
    INNER JOIN map_namespace_internal
      ON map_namespace_lineage.dim_namespace_ultimate_parent_id = map_namespace_internal.ultimate_parent_namespace_id
    WHERE is_namespace_internal

),  gitlab_issue_description_parsing AS (

    SELECT
      issue_id,
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_description, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}') AS sfdc_link_array,
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_description, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}')      AS zendesk_link_array,
      SPLIT_PART(REGEXP_SUBSTR(issue_description, '~"customer priority::[0-9]{1,2}'), '::', -1)::NUMBER                                     AS request_priority
    FROM issue_extended
    WHERE issue_description IS NOT NULL
      AND NOT (ARRAY_SIZE(sfdc_link_array) = 0 AND ARRAY_SIZE(zendesk_link_array) = 0)

), issue_notes_extended AS (

    SELECT issue_notes.*
    FROM issue_notes
    INNER JOIN issue_extended
      ON issue_notes.issue_id = issue_extended.issue_id

), gitlab_issue_notes_parsing AS (

    SELECT
      note_id,
      issue_id,
      "{{this.database}}".{{target.schema}}.regexp_to_array(note, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}') AS sfdc_link_array,
      "{{this.database}}".{{target.schema}}.regexp_to_array(note, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}')      AS zendesk_link_array,
      SPLIT_PART(REGEXP_SUBSTR(note, '~"customer priority::[0-9]{1,2}'), '::', -1)::NUMBER                                     AS request_priority,
      created_at                                                                                                               AS note_created_at,
      updated_at                                                                                                               AS note_updated_at
    FROM issue_notes_extended
    WHERE NOT (ARRAY_SIZE(sfdc_link_array) = 0 AND ARRAY_SIZE(zendesk_link_array) = 0)

), gitlab_issue_notes_sfdc_links AS (

    SELECT
      note_id,
      issue_id,
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
    FROM gitlab_issue_notes_parsing, 
      TABLE(FLATTEN(sfdc_link_array)) f

), gitlab_issue_description_sfdc_links AS (

    SELECT
      issue_id,
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
    FROM gitlab_issue_description_parsing, 
    TABLE(FLATTEN(sfdc_link_array)) f

), gitlab_issue_notes_zendesk_link AS (

    SELECT
      note_id,
      issue_id,
      REPLACE(f.value, '"', '')                      AS dim_ticket_id,
      'Zendesk Ticket'                               AS link_type,
      request_priority,
      note_created_at,
      note_updated_at
    FROM gitlab_issue_notes_parsing, 
      TABLE(FLATTEN(zendesk_link_array)) f

), gitlab_issue_notes_zendesk_with_sfdc_account AS (

    SELECT
      gitlab_issue_notes_zendesk_link.*,
      zendesk_organization.sfdc_account_id AS dim_crm_account_id
    FROM gitlab_issue_notes_zendesk_link
    LEFT JOIN zendesk_ticket
      ON zendesk_ticket.ticket_id = gitlab_issue_notes_zendesk_link.dim_ticket_id
    LEFT JOIN zendesk_organization
      ON zendesk_organization.organization_id = zendesk_ticket.organization_id

), gitlab_issue_description_zendesk_link AS (

    SELECT
      issue_id,
      REPLACE(f.value, '"', '')                      AS dim_ticket_id,
      'Zendesk Ticket'                               AS link_type,
      request_priority
    FROM gitlab_issue_description_parsing, 
        TABLE(FLATTEN(zendesk_link_array)) f

), gitlab_issue_description_zendesk_with_sfdc_account AS (

    SELECT
      gitlab_issue_description_zendesk_link.*,
      zendesk_organization.sfdc_account_id AS dim_crm_account_id
    FROM gitlab_issue_description_zendesk_link
    LEFT JOIN zendesk_ticket
      ON zendesk_ticket.ticket_id = gitlab_issue_description_zendesk_link.dim_ticket_id
    LEFT JOIN zendesk_organization
      ON zendesk_organization.organization_id = zendesk_ticket.organization_id

), union_links AS (

    SELECT
      issue_id AS dim_issue_id,
      link_type,
      dim_crm_opportunity_id,
      dim_crm_account_id,
      NULL AS dim_ticket_id,
      IFNULL(request_priority, 1)::NUMBER AS request_priority
    FROM gitlab_issue_notes_sfdc_links
    QUALIFY ROW_NUMBER() OVER(PARTITION BY issue_id, sfdc_id_18char ORDER BY note_created_at DESC) = 1

    UNION

    SELECT
      issue_id,
      link_type,
      NULL dim_crm_opportunity_id,
      dim_crm_account_id,
      dim_ticket_id,
      IFNULL(request_priority, 1)::NUMBER AS request_priority
    FROM gitlab_issue_notes_zendesk_with_sfdc_account
    QUALIFY ROW_NUMBER() OVER(PARTITION BY issue_id, dim_ticket_id ORDER BY note_created_at DESC) = 1

    UNION

    SELECT
      gitlab_issue_description_sfdc_links.issue_id,
      gitlab_issue_description_sfdc_links.link_type,
      gitlab_issue_description_sfdc_links.dim_crm_opportunity_id,
      gitlab_issue_description_sfdc_links.dim_crm_account_id,
      NULL AS dim_ticket_id,
      IFNULL(gitlab_issue_description_sfdc_links.request_priority, 1)::NUMBER AS request_priority
    FROM gitlab_issue_description_sfdc_links
    LEFT JOIN gitlab_issue_notes_sfdc_links
      ON gitlab_issue_description_sfdc_links.issue_id = gitlab_issue_notes_sfdc_links.issue_id
      AND gitlab_issue_description_sfdc_links.sfdc_id_18char = gitlab_issue_notes_sfdc_links.sfdc_id_18char
    WHERE gitlab_issue_notes_sfdc_links.issue_id IS NULL

    UNION

    SELECT
      gitlab_issue_description_zendesk_with_sfdc_account.issue_id,
      gitlab_issue_description_zendesk_with_sfdc_account.link_type,
      NULL dim_crm_opportunity_id,
      gitlab_issue_description_zendesk_with_sfdc_account.dim_crm_account_id,
      gitlab_issue_description_zendesk_with_sfdc_account.dim_ticket_id,
      IFNULL(gitlab_issue_description_zendesk_with_sfdc_account.request_priority, 1)::NUMBER AS request_priority
    FROM gitlab_issue_description_zendesk_with_sfdc_account
    LEFT JOIN gitlab_issue_notes_zendesk_link
      ON gitlab_issue_description_zendesk_with_sfdc_account.issue_id = gitlab_issue_notes_zendesk_link.issue_id
      AND gitlab_issue_description_zendesk_with_sfdc_account.dim_ticket_id = gitlab_issue_notes_zendesk_link.dim_ticket_id
    WHERE gitlab_issue_notes_zendesk_link.issue_id IS NULL

), final AS (

    SELECT
      map_moved_issue.dim_issue_id,
      union_links.link_type,
      union_links.dim_crm_opportunity_id,
      union_links.dim_crm_account_id,
      union_links.dim_ticket_id,
      union_links.request_priority
    FROM union_links
    INNER JOIN map_moved_issue
      ON map_moved_issue.issue_id = union_links.dim_issue_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-10-12",
    updated_date="2021-10-12",
) }}
