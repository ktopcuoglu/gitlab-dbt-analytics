{{config({
    "schema": "common"
  })
}}

WITH sfdc_leads AS (

    SELECT *
    FROM {{ ref('sfdc_lead') }}

), sfdc_contacts AS (

    SELECT *
    FROM {{ ref('sfdc_contact') }}

), unioned AS (

    SELECT
      --id
      {{ dbt_utils.surrogate_key(['contact_id']) }} AS crm_person_id,
      contact_id                                    AS sfdc_record_id,
      'contact'                                     AS sfdc_record_type,
      contact_email_hash                            AS email_hash,
      email_domain,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      account_id                                    AS crm_account_id,
      reports_to_id,
      owner_id                                      AS crm_sales_rep_id,

      --info
      person_score,
      contact_title                                 AS title,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      lead_source,
      lead_source_type,
      source_buckets,
      net_new_source_categories

    FROM sfdc_contacts

    UNION

    SELECT
      --id
      {{ dbt_utils.surrogate_key(['lead_id']) }} AS crm_person_id,
      lead_id                                    AS sfdc_record_id,
      'lead'                                     AS sfdc_record_type,
      lead_email_hash                            AS email_hash,
      email_domain,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      lean_data_matched_account                  AS crm_account_id,
      NULL                                       AS reports_to_id,
      owner_id                                   AS crm_sales_rep_id,

      --info
      person_score,
      title,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      lead_source,
      lead_source_type,
      source_buckets,
      net_new_source_categories

    FROM sfdc_leads
    WHERE is_converted = false

)

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@jjstark",
    updated_by="@iweeks",
    created_date="2020-09-10",
    updated_date="2020-11-19"
) }}
