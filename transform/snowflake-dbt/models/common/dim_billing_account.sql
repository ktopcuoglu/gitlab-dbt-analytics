{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_contact','zuora_contact_source')
]) }}

, zuora_account AS (

    SELECT *
    FROM {{ref('zuora_account_source')}}
    --Exclude Batch20 which are the test accounts. This method replaces the manual dbt seed exclusion file.
    WHERE LOWER(batch) != 'batch20'
      AND is_deleted = FALSE

), filtered AS (

    SELECT
      zuora_account.account_id                              AS dim_billing_account_id,
      map_merged_crm_account.dim_crm_account_id            AS dim_crm_account_id,
      zuora_account.account_number                          AS billing_account_number,
      zuora_account.account_name                            AS billing_account_name,
      zuora_account.status                                  AS account_status,
      zuora_account.parent_id,
      zuora_account.sfdc_account_code,
      zuora_account.currency                                AS account_currency,
      zuora_contact.country                                 AS sold_to_country,
      zuora_account.ssp_channel,
      zuora_account.po_required,
      zuora_account.is_deleted,
      zuora_account.batch
    FROM zuora_account
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN map_merged_crm_account
      ON zuora_account.crm_id = map_merged_crm_account.sfdc_account_id

)

{{ dbt_audit(
    cte_ref="filtered",
    created_by="@msendal",
    updated_by="@jpeguero",
    created_date="2020-07-20",
    updated_date="2021-10-21"
) }}
