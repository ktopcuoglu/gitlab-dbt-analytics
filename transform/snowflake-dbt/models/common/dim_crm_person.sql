WITH crm_person AS (

    SELECT *
    FROM {{ ref('prep_crm_person') }}

), final AS (

    SELECT
      --id
      dim_crm_person_id,
      sfdc_record_id,
      bizible_person_id,
      sfdc_record_type,
      email_hash,
      email_domain,
      email_domain_type,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      dim_crm_account_id,
      reports_to_id,
      dim_crm_user_id,
      crm_partner_id,

      --info
      person_score,
      title,
      country,
      mailing_country,
      state,
      mailing_state,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      status,
      lead_source,
      lead_source_type,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      sequence_step_type,
      is_actively_being_sequenced,
      region,
      marketo_last_interesting_moment,
      marketo_last_interesting_moment_date,
      outreach_step_number,
      matched_account_owner_role,
      matched_account_account_owner_name,
      matched_account_sdr_assigned,
      matched_account_type,
      matched_account_gtm_strategy

    FROM crm_person
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jjstark",
    updated_by="@degan",
    created_date="2020-09-10",
    updated_date="2021-12-15"
) }}
