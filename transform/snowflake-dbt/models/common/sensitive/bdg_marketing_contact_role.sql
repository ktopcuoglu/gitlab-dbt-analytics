WITH gitlab_namespaces AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces_source') }}

), gitlab_members AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_members_source') }}

), gitlab_users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users_source') }}

), customer_db_source AS (

    SELECT *
    FROM {{ ref('customers_db_customers_source') }}

),zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

),zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact_source') }}

),dim_marketing_contact AS (

    SELECT *
    FROM {{ ref('dim_marketing_contact') }}

), final AS (

    SELECT
      dim_marketing_contact_id,
      gitlab_users.notification_email                             AS email_address,
      owner_id                                                    AS user_id,
      NULL                                                        AS customer_db_customer_id,
      namespace_id                                                AS namespace_id,
      NULL                                                        AS zuora_billing_account_id,
      'Personal Namespace Owner'                                  AS marketing_contact_role
    FROM gitlab_namespaces
    INNER JOIN gitlab_users 
      ON gitlab_users.user_id = gitlab_namespaces.owner_id
    LEFT JOIN dim_marketing_contact
      ON dim_marketing_contact.email_address = gitlab_users.notification_email 
    WHERE owner_id IS NOT NULL
      AND namespace_type IS NULL
      AND parent_id IS NULL
  
    UNION ALL

    SELECT DISTINCT
      dim_marketing_contact_id,
      gitlab_users.notification_email                             AS email_address,
      gitlab_users.user_id                                        AS user_id,
      NULL                                                        AS customer_db_customer_id,
      gitlab_members.source_id                                    AS namespace_id,
      NULL                                                        AS zuora_billing_account_id,
      'Group Namespace Owner'                                     AS marketing_contact_role
      FROM gitlab_members
      INNER JOIN gitlab_users
        ON gitlab_users.user_id = gitlab_members.user_id
      LEFT JOIN dim_marketing_contact
        ON dim_marketing_contact.email_address = gitlab_users.notification_email
      WHERE gitlab_members.member_source_type = 'Namespace'
        AND gitlab_members.access_level = 50

    UNION ALL

    SELECT DISTINCT
      dim_marketing_contact_id,
      gitlab_users.notification_email                             AS email_address,
      gitlab_users.user_id                                        AS user_id,
      NULL                                                        AS customer_db_customer_id,
      gitlab_members.source_id                                    AS namespace_id,
      NULL                                                        AS zuora_billing_account_id,
      'Group Namespace Member'                                    AS marketing_contact_role
    FROM gitlab_members
    INNER JOIN gitlab_users
      ON gitlab_users.user_id = gitlab_members.user_id
    LEFT JOIN dim_marketing_contact
      ON dim_marketing_contact.email_address = gitlab_users.notification_email
    WHERE gitlab_members.member_source_type = 'Namespace'
      AND gitlab_members.access_level <> 50

    UNION ALL

    SELECT
      dim_marketing_contact_id,
      customer_db_source.customer_email                           AS email_address,
      NULL                                                        AS user_id,
      customer_id                                                 AS customer_db_customer_id,
      NULL                                                        AS namespace_id,
      CAST(NULL as varchar)                                       AS zuora_billing_account_id,
      'Customer DB Owner'                                         AS marketing_contact_role
    FROM customer_db_source
    LEFT JOIN dim_marketing_contact
      ON dim_marketing_contact.email_address = customer_db_source.customer_email
  
   UNION ALL

    SELECT
      dim_marketing_contact_id,
      zuora_contact.work_email                                   AS email_address,
      NULL                                                       AS user_id,
      NULL                                                       AS customer_db_customer_id,
      NULL                                                       AS namespace_id,
      zuora_account.account_id                                   AS zuora_billing_account_id,
      'Zuora Billing Contact'                                    AS marketing_contact_role
    FROM zuora_account
    INNER JOIN zuora_contact
      ON zuora_contact.account_id = zuora_account.account_id
    LEFT JOIN dim_marketing_contact
      ON dim_marketing_contact.email_address = zuora_contact.work_email
    
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@rmistry",
    updated_by="@trevor31",
    created_date="2021-01-19",
    updated_date="2021-02-22"
) }}
