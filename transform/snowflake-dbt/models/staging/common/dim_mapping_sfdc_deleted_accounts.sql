WITH sfdc_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_account_source') }}
    WHERE account_id IS NOT NULL

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), deleted_accounts AS (

    SELECT *
    FROM sfdc_accounts
    WHERE is_deleted = TRUE
      AND account_id IS NOT NULL

), master_records AS (

    SELECT
      a.account_id                                     AS sfdc_account_id,
      COALESCE(b.master_record_id, a.master_record_id) AS sfdc_master_record_id
    FROM deleted_accounts a
    LEFT JOIN deleted_accounts b
      ON a.master_record_id = b.account_id

), initial_join_to_sfdc AS (

    SELECT DISTINCT
      zuora_account.account_id  AS zuora_account_id,
      zuora_account.crm_id      AS zuora_crm_id,
      sfdc_accounts.account_id  AS sfdc_account_id_int,
      sfdc_accounts.is_deleted
    FROM zuora_account
    INNER JOIN sfdc_accounts
      ON zuora_account.crm_id = sfdc_accounts.account_id
    WHERE sfdc_accounts.account_id IS NOT NULL

), replace_sfdc_account_id AS (

    SELECT
      initial_join_to_sfdc.zuora_account_id,
      initial_join_to_sfdc.sfdc_account_id_int        AS crm_id_original,
      CASE
        WHEN initial_join_to_sfdc.is_deleted
        THEN master_records.sfdc_master_record_id
        ELSE initial_join_to_sfdc.sfdc_account_id_int
      END                                             AS crm_id_final,
      initial_join_to_sfdc.is_deleted                 AS crm_id_original_is_deleted
    FROM initial_join_to_sfdc
    LEFT JOIN master_records
      ON initial_join_to_sfdc.sfdc_account_id_int = master_records.sfdc_account_id

)

SELECT *
FROM replace_sfdc_account_id
