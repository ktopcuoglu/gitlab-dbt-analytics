WITH sfdc_lead AS(

  SELECT *
  FROM {{ ref('sfdc_lead') }}

), sfdc_contact AS (

	SELECT *
	FROM {{ ref('sfdc_contact')}}

), marketing_qualification_events AS(

SELECT

    {{ dbt_utils.surrogate_key(['lead_id','marketo_qualified_lead_date::timestamp']) }} AS event_id,
    marketo_qualified_lead_date::timestamp                                              AS event_timestamp,
    lead_id                                                                             AS sfdc_record_id,
    'lead'                                                                              AS sfdc_record,
    {{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, lead_id)']) }}          AS crm_person_id,
    converted_contact_id                                                                AS contact_id,
    converted_account_id                                                                AS account_id,
    'marketing qualification'                                                           AS event_name
  
  FROM sfdc_lead
  WHERE marketo_qualified_lead_date IS NOT NULL

  UNION 

  SELECT

    {{ dbt_utils.surrogate_key(['contact_id','marketo_qualified_lead_date::timestamp']) }} AS event_id,
    marketo_qualified_lead_date::timestamp                                                 AS event_timestamp,
    contact_id                                                                             AS sfdc_record_id,
    'contact'                                                                              AS sfdc_record,
    {{ dbt_utils.surrogate_key(['contact_id']) }}                                          AS crm_person_id,
    contact_id                                                                             AS contact_id,
    account_id                                                                             AS account_id,
    'marketing qualification'                                                              AS event_name
     
  FROM sfdc_contact
  WHERE marketo_qualified_lead_date IS NOT NULL

)

SELECT *
FROM marketing_qualification_events