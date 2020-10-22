{{
  config( materialized='ephemeral')
}}

WITH map_merged_crm_accounts AS (

    SELECT *
    FROM {{ ref('map_merged_crm_accounts') }}

), sfdc_account AS (

    SELECT *
    FROM {{ ref('sfdc_account_snapshots_source') }}
    WHERE account_id IS NOT NULL
      AND '{{ var('valid_at') }}'::TIMESTAMP_TZ >= dbt_valid_from
      AND '{{ var('valid_at') }}'::TIMESTAMP_TZ < {{ coalesce_to_infinity('dbt_valid_to') }}

), sfdc_users AS (

    SELECT *
    FROM {{ ref('sfdc_user_snapshots_source') }}
    WHERE '{{ var('valid_at') }}'::TIMESTAMP_TZ >= dbt_valid_from
      AND '{{ var('valid_at') }}'::TIMESTAMP_TZ < {{ coalesce_to_infinity('dbt_valid_to') }}

), ultimate_parent_account AS (

    SELECT
      account_id,
      account_name,
      account_segment,
      billing_country,
      df_industry,
      account_owner_team,
      tsp_territory
    FROM sfdc_account
    WHERE account_id = ultimate_parent_account_id

)

SELECT
  sfdc_account.account_id                       AS crm_account_id,
  sfdc_account.account_name                     AS crm_account_name,
  sfdc_account.billing_country                  AS crm_account_country,
  ultimate_parent_account.account_id            AS ultimate_parent_account_id,
  ultimate_parent_account.account_name          AS ultimate_parent_account_name,
  {{ sales_segment_cleaning('ultimate_parent_account.account_segment') }}
                                                AS ultimate_parent_account_segment,
  ultimate_parent_account.billing_country       AS ultimate_parent_billing_country,
  ultimate_parent_account.df_industry           AS ultimate_parent_industry,
  ultimate_parent_account.account_owner_team    AS ultimate_parent_account_owner_team,
  ultimate_parent_account.tsp_territory         AS ultimate_parent_territory,
  sfdc_account.record_type_id                   AS record_type_id,
  sfdc_account.federal_account                  AS federal_account,
  sfdc_account.gitlab_com_user,
  sfdc_account.account_owner,
  sfdc_account.account_owner_team,
  sfdc_account.account_type,
  sfdc_users.name                               AS technical_account_manager,
  sfdc_account.is_deleted                       AS is_deleted,
  map_merged_crm_accounts.dim_crm_account_id    AS merged_to_account_id
FROM sfdc_account
LEFT JOIN map_merged_crm_accounts
  ON sfdc_account.account_id = map_merged_crm_accounts.sfdc_account_id
LEFT JOIN ultimate_parent_account
  ON ultimate_parent_account.account_id = sfdc_account.ultimate_parent_account_id
LEFT OUTER JOIN sfdc_users
  ON sfdc_account.technical_account_manager_id = sfdc_users.user_id
