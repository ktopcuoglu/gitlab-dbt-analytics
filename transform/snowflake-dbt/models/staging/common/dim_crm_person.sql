{{config({
    "schema": "common"
  })
}}

WITH crm_person AS (

    SELECT *
    FROM {{ ref('base_crm_person') }}

), final AS (

    SELECT
      --id
      dim_crm_person_id,
      sfdc_record_id,
      bizible_person_id,
      sfdc_record_type,
      email_hash,
      email_domain,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      dim_crm_account_id,
      reports_to_id,
      dim_crm_sales_rep_id,

      --info
      person_score,
      title,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      lead_source,
      lead_source_type,
      source_buckets,
      net_new_source_categories,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date

    FROM crm_person
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jjstark",
    updated_by="@mcooperDD",
    created_date="2020-09-10",
    updated_date="2020-12-15"
) }}
