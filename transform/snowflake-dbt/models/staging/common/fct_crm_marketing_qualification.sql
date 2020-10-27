WITH sfdc_lead AS(

    SELECT *
    FROM {{ ref('sfdc_lead') }}

), sfdc_contact AS (

	SELECT *
	FROM {{ ref('sfdc_contact')}}

), marketing_qualified_leads AS(

SELECT

    {{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, lead_id)','marketo_qualified_lead_date::timestamp']) }} AS event_id,
    marketo_qualified_lead_date::timestamp                                              AS event_timestamp,
    {{ get_date_id('marketo_qualified_lead_date') }}                                    AS date_id,
    lead_id                                                                             AS sfdc_record_id,
    'lead'                                                                              AS sfdc_record,
    {{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, lead_id)']) }}          AS crm_person_id,
    converted_contact_id                                                                AS contact_id,
    converted_account_id                                                                AS account_id,
    owner_id                                                                            AS crm_sales_rep_id
  
  FROM sfdc_lead
  WHERE marketo_qualified_lead_date IS NOT NULL

), marketing_qualified_contacts AS(

  SELECT 

    {{ dbt_utils.surrogate_key(['contact_id','marketo_qualified_lead_date::timestamp']) }} AS event_id,
    marketo_qualified_lead_date::timestamp                                                 AS event_timestamp,
    {{ get_date_id('marketo_qualified_lead_date') }}                                       AS date_id,
    contact_id                                                                             AS sfdc_record_id,
    'contact'                                                                              AS sfdc_record,
    {{ dbt_utils.surrogate_key(['contact_id']) }}                                          AS crm_person_id,
    contact_id                                                                             AS contact_id,
    account_id                                                                             AS account_id,
    owner_id                                                                               AS crm_sales_rep_id
     
  FROM sfdc_contact
  WHERE marketo_qualified_lead_date IS NOT NULL
  HAVING event_id not in (
                         SELECT event_id
                         FROM marketing_qualified_leads
                         ) 

), unioned AS(

  SELECT *
  FROM marketing_qualified_leads

  UNION

  SELECT *
  FROM marketing_qualified_contacts

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@jjstark ",
    updated_by="@jjstark",
    created_date="2020-09-09",
    updated_date="2020-09-25"
) }}
