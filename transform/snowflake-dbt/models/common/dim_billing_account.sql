WITH map_merged_crm_accounts AS (

    SELECT *
    FROM {{ ref('map_merged_crm_accounts') }}

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_source') }}

), excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM {{ref('zuora_excluded_accounts')}}

), filtered AS (

    SELECT
      zuora_account.account_id                              AS dim_billing_account_id,
      map_merged_crm_accounts.dim_crm_account_id            AS dim_crm_account_id,
      zuora_account.account_number                          AS billing_account_number,
      zuora_account.account_name                            AS billing_account_name,
      zuora_account.status                                  AS account_status,
      zuora_account.parent_id,
      zuora_account.sfdc_account_code,
      zuora_account.currency                                AS account_currency,
      zuora_contact.country                                 AS sold_to_country,
      zuora_account.is_deleted,
      zuora_account.account_id IN (
                                    SELECT
                                      account_id
                                    FROM excluded_accounts
                                  ) AS is_excluded
    FROM zuora_account
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact.contact_id
    LEFT JOIN map_merged_crm_accounts
      ON zuora_account.crm_id = map_merged_crm_accounts.sfdc_account_id
    WHERE zuora_account.is_deleted = FALSE

)

{{ dbt_audit(
    cte_ref="filtered",
    created_by="@msendal",
    updated_by="@mcooperDD",
    created_date="2020-07-20",
    updated_date="2020-12-07"
) }}
