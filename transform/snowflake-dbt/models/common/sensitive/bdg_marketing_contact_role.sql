WITH gitlab_namespaces AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_namespaces_source') }}

), gitlab_members AS (
    SELECT *
    FROM {{ref('gitlab_dotcom_members_source') }}

), gitlab_users AS (
    SELECT *
    FROM {{ref('gitlab_dotcom_users_source') }}

), customer_db_source AS (
    SELECT *
    FROM {{ref('customers_db_customers_source') }}

), zuora_subscription AS (
    SELECT *
    FROM {{ref('zuora_subscription_source') }}

),zuora_account AS (
    SELECT *
    FROM {{ref('zuora_account_source') }}

), final
    SELECT
      owner_id                                                    AS "user_id",
      NULL                                                        AS "customer_db_customer_id",
      namespace_id                                                AS "namespace_id",
      NULL                                                        AS "zuora_subscription_id",
      CAST(NULL as varchar(100))                                  AS "zuora_billing_contact_id",
      'Personal Namespace Owner'                                  AS "marketing_contact_role"
    FROM gitlab_namespaces
    WHERE owner_id IS NOT NULL
      AND namespace_type IS NULL
      AND parent_id IS NULL

    UNION ALL

    SELECT
      gitlab_users.user_id                                        AS "user_id",
      NULL                                                        AS "customer_db_customer_id",
      gitlab_members.source_id                                    AS "namespace_id",
      NULL                                                        AS "zuora_subscription_id",
      CAST(NULL as varchar(100))                                  AS "zuora_billing_contact_id",
      'Group Namespace Owner'                                     AS "marketing_contact_role"
      FROM gitlab_members
      JOIN gitlab_users
        ON gitlab_users.user_id = gitlab_members.user_id
      WHERE gitlab_members.member_source_type = 'Namespace'
        AND gitlab_members.access_level = 50

    UNION ALL

    SELECT
      gitlab_users.user_id                                        AS "user_id",
      NULL                                                        AS "customer_db_customer_id",
      gitlab_members.source_id                                    AS "namespace_id",
      NULL                                                        AS "zuora_subscription_id",
      CAST(NULL as varchar(100))                                  AS "zuora_billing_contact_id",
      'Group Namespace Member'                                    AS "marketing_contact_role"
    FROM gitlab_members
    JOIN gitlab_users
      ON gitlab_users.user_id = gitlab_members.user_id
    WHERE gitlab_members.member_source_type = 'Namespace'
      AND gitlab_members.access_level <> 50

    UNION ALL

    SELECT
      NULL                                                        AS "user_id",
      customer_id                                                 AS "customer_db_customer_id",
      NULL                                                        AS "namespace_id",
      NULL                                                        AS "zuora_subscription_id",
      CAST(NULL as varchar(100))                                  AS "zuora_billing_contact_id",
      'Customer DB Owner'                                         AS "marketing_contact_role"
    FROM customer_db_source

    UNION ALL

    SELECT
      NULL                                                       AS "user_id",
      NULL                                                       AS "customer_db_customer_id",
      NULL                                                       AS "namespace_id",
      zuora_subscription.subscription_id                         AS "zuora_subscription_id",
      zuora_account.bill_to_contact_id                           AS "zuora_billing_contact_id",
      'Zuora Billing Contact'                                    AS "marketing_contact_role"
    FROM zuora_subscription
    JOIN zuora_account
      ON zuora_account.account_id = zuora_subscription.account_id
    WHERE zuora_subscription.subscription_status = 'Active'
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rmistry",
    updated_by="@rmistry",
    created_date="2021-01-19",
    updated_date="2021-01-19"
) }}