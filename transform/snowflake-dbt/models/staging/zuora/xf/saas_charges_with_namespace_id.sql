WITH zuora_base_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr') }}
    WHERE delivery = 'SaaS'

)

, customers_db_charges AS (

    SELECT *
    FROM {{ ref('customers_db_charges_xf') }}

)

, namespaces AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces') }}

)

, dim_billing_accounts AS (

    SELECT *
    FROM {{ ref('dim_billing_accounts') }}

)

, dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

)

, dim_subscriptions AS (

    SELECT *
    FROM {{ ref('dim_subscriptions') }}

)


, joined AS (

    SELECT
      zuora_base_mrr.rate_plan_charge_id,
      zuora_base_mrr.subscription_name_slugify,
      dim_billing_accounts.billing_account_id                               AS zuora_account_id,
      COALESCE(merged_accounts.crm_account_id, dim_crm_account.crm_account_id) AS sfdc_account_id,
      COALESCE(merged_accounts.ultimate_parent_account_id,
                dim_crm_account.ultimate_parent_account_id)    AS ultimate_parent_account_id,
      COALESCE(merged_accounts.ultimate_parent_account_name,
                dim_crm_account.ultimate_parent_account_name)  AS ultimate_parent_account_name,
      customers_db_charges.current_customer_id,
      namespaces.namespace_id
    FROM zuora_base_mrr
    LEFT JOIN customers_db_charges
      ON zuora_base_mrr.rate_plan_charge_id = customers_db_charges.rate_plan_charge_id
    LEFT JOIN namespaces
      ON customers_db_charges.current_gitlab_namespace_id = namespaces.namespace_id
    LEFT JOIN dim_billing_accounts
      ON zuora_base_mrr.account_number = dim_billing_accounts.billing_account_number
    LEFT JOIN dim_crm_account
      ON dim_billing_accounts.crm_account_id = dim_crm_account.crm_account_id
    LEFT JOIN dim_crm_account AS merged_accounts
      ON dim_crm_account.merged_to_account_id = merged_accounts.crm_account_id

)

SELECT *
FROM joined
