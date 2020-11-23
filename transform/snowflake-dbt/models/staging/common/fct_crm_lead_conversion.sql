WITH sfdc_lead AS(

  SELECT *
  FROM {{ ref('sfdc_lead') }}

), sfdc_lead_history AS(

  SELECT *
  FROM {{ ref('sfdc_lead_history') }}

), lead_conversion_event AS (

  SELECT
    
    {{ dbt_utils.surrogate_key(['lead_history_id','field_modified_at']) }}               AS event_id,
    sfdc_lead_history.field_modified_at                                                  AS event_timestamp,
    {{ get_date_id('field_modified_at') }}                                               AS date_id,
    sfdc_lead_history.lead_history_id                                                    AS sfdc_record_id,
    'lead history'                                                                       AS sfdc_record,
    {{ dbt_utils.surrogate_key(['COALESCE(converted_contact_id, sfdc_lead.lead_id)']) }} AS crm_person_id,
    sfdc_lead_history.lead_id                                                            AS lead_id,
    sfdc_lead_history.created_by_id                                                      AS crm_user_id,
    sfdc_lead.converted_contact_id                                                       AS contact_id,
    sfdc_lead.converted_account_id                                                       AS crm_account_id,
    sfdc_lead.converted_opportunity_id                                                   AS opportunity_id

  FROM sfdc_lead_history
  INNER JOIN sfdc_lead
    ON sfdc_lead.lead_id = sfdc_lead_history.lead_id

)


{{ dbt_audit(
    cte_ref="lead_conversion_event",
    created_by="@jjstark ",
    updated_by="@jjstark",
    created_date="2020-09-09",
    updated_date="2020-09-25"
) }}
