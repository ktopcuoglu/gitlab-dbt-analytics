WITH biz_person AS (

    SELECT *
    FROM {{ref('sfdc_bizible_person_source')}}
    WHERE is_deleted = 'FALSE'

), biz_touchpoints AS (

    SELECT *
    FROM {{ref('sfdc_bizible_touchpoint_source')}}
    WHERE bizible_touchpoint_position LIKE '%FT%'
     AND is_deleted = 'FALSE'

), biz_person_with_touchpoints AS (

    SELECT

      biz_touchpoints.*,
      biz_person.bizible_contact_id,
      biz_person.bizible_lead_id

    FROM biz_touchpoints
    JOIN biz_person
      ON biz_touchpoints.bizible_person_id = biz_person.person_id

), sfdc_contacts AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_contact_source') }}
    FROM {{ref('sfdc_contact_source')}}
    WHERE is_deleted = 'FALSE'

), sfdc_leads AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_lead_source') }}
    FROM {{ref('sfdc_lead_source')}}
    WHERE is_deleted = 'FALSE'

), crm_person_final AS (

    SELECT
      --id
      {{ dbt_utils.surrogate_key(['contact_id']) }} AS dim_crm_person_id,
      contact_id                                    AS sfdc_record_id,
      bizible_person_id                             AS bizible_person_id,
      'contact'                                     AS sfdc_record_type,
      contact_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      account_id                                    AS dim_crm_account_id,
      reports_to_id,
      owner_id                                      AS dim_crm_user_id,

      --info
      person_score,
      contact_title                                 AS title,
      it_job_title_hierarchy,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      contact_status                                AS status,
      lead_source,
      lead_source_type,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      region,
      NULL                                          AS country,
      mailing_country,
      last_activity_date,

      NULL                                          AS crm_partner_id

    FROM sfdc_contacts
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_contacts.contact_id = biz_person_with_touchpoints.bizible_contact_id

    UNION

    SELECT
      --id
      {{ dbt_utils.surrogate_key(['lead_id']) }} AS dim_crm_person_id,
      lead_id                                    AS sfdc_record_id,
      bizible_person_id                          AS bizible_person_id,
      'lead'                                     AS sfdc_record_type,
      lead_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      lean_data_matched_account                  AS dim_crm_account_id,
      NULL                                       AS reports_to_id,
      owner_id                                   AS dim_crm_user_id,

      --info
      person_score,
      title,
      it_job_title_hierarchy,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      lead_status                                AS status,
      lead_source,
      lead_source_type,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      region,
      country,
      NULL                                      AS mailing_country,
      last_activity_date,

      crm_partner_id

    FROM sfdc_leads
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_leads.lead_id = biz_person_with_touchpoints.bizible_lead_id
    WHERE is_converted = 'FALSE'

), duplicates AS (

    SELECT 
      dim_crm_person_id
    FROM crm_person_final
    GROUP BY 1
    HAVING COUNT(*) > 1

), final AS (

    SELECT *
    FROM crm_person_final
    WHERE dim_crm_person_id NOT IN (
                                    SELECT *
                                    FROM duplicates
                                      )

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@michellecooper",
    created_date="2020-12-08",
    updated_date="2021-10-27"
) }}
