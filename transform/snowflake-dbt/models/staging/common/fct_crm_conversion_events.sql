WITH sfdc_lead AS(

  SELECT *
  FROM {{ ref('sfdc_lead') }}

), sfdc_lead_history_source AS(

  SELECT *
  FROM {{ ref('sfdc_lead_history_source') }}

), conversion_events AS (

  SELECT -- lead conversion
    
    {{ dbt_utils.surrogate_key(['lead_history_id','field_modified_at']) }}               AS event_id,
    sfdc_lead_history_source.field_modified_at                                           AS event_timestamp,
	sfdc_lead_history_source.lead_history_id                                             AS sfdc_record_id,
	'lead history'                                                                       AS sfdc_record,
	{{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, sfdc_lead.lead_id)']) }} AS crm_person_id,
    sfdc_lead_history_source.lead_id                                                     AS lead_id,
    sfdc_lead_history_source.created_by_id                                               AS crm_user_id,
    sfdc_lead.converted_contact_id                                                       AS contact_id,
    sfdc_lead.converted_account_id                                                       AS account_id,
    sfdc_lead.converted_opportunity_id                                                   AS opportunity_id,
    'lead conversion'                                                                    AS event_name

  FROM sfdc_lead_history_source
  INNER JOIN sfdc_lead
    ON sfdc_lead.lead_id = sfdc_lead_history_source.lead_id

  UNION

  SELECT -- marketing qualification

    {{ dbt_utils.surrogate_key(['lead_id','marketo_qualified_lead_date::timestamp']) }} AS event_id,
    marketo_qualified_lead_date::timestamp                                              AS event_timestamp,
	lead_id                                                                             AS sfdc_record_id,
	'lead'                                                                              AS sfdc_record,
	{{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, lead_id)']) }}          AS crm_person_id,
    lead_id                                                                             AS lead_id,
    NULL                                                                                AS crm_user_id, -- if we move this to lead history then we can get this
    converted_contact_id                                                                AS contact_id,
    converted_opportunity_id                                                            AS opportunity_id,
    converted_account_id                                                                AS account_id,
    'marketing qualification'                                                           AS event_name
  
  FROM sfdc_lead
  WHERE marketo_qualified_lead_date IS NOT NULL

)

SELECT *
FROM conversion_events