WITH sfdc_lead AS (

    SELECT *
    FROM {{ref('sfdc_lead_source') }}

), sfdc_contact AS (

    SELECT *
    FROM {{ref('sfdc_contact_source') }}

), sfdc_account AS (

    SELECT *
    FROM {{ref('sfdc_account_source') }}

), crm_account AS (

    SELECT *
    FROM {{ref('map_crm_account') }}

), sales_segment AS (

    SELECT *
    FROM {{ref('dim_sales_segment') }}

), crm_person AS (

    SELECT *
    FROM {{ref('prep_crm_person') }}

), gitlab_users AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_users_source') }}

), customer_db_source AS (

    SELECT *
    FROM {{ref('customers_db_customers_source') }}

), zuora_contact_source AS (

    SELECT *
    FROM {{ref('zuora_contact_source') }}

), zuora_account_source AS (

    SELECT *
    FROM {{ref('zuora_account_source') }}

), sfdc AS (

    SELECT
      sfdc_lead.lead_id,
      CASE WHEN crm_person.sfdc_record_type = 'contact' THEN sfdc_contact.contact_email ELSE sfdc_lead.lead_email END        AS email_address,
      crm_person.dim_crm_person_id                                                                                           AS crm_person_id,
      crm_person.sfdc_record_type                                                                                            AS sfdc_lead_contact,
      CASE
        WHEN sfdc_lead_contact = 'contact' THEN sfdc_contact.contact_first_name
        ELSE sfdc_lead.lead_first_name
      END                                                                                                                    AS first_name,
      CASE
        WHEN sfdc_lead_contact = 'contact' AND sfdc_contact.contact_last_name  = '[[unknown]]' THEN NULL
        WHEN sfdc_lead_contact = 'contact' AND sfdc_contact.contact_last_name  <> '[[unknown]]' THEN sfdc_contact.contact_last_name
        WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.lead_last_name = '[[unknown]]' THEN NULL
        WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.lead_last_name <> '[[unknown]]' THEN sfdc_lead.lead_last_name
      END                                                                                                                    AS last_name,
      CASE
        WHEN sfdc_lead_contact = 'contact' AND sfdc_account.account_name = '[[unknown]]' THEN NULL
        WHEN sfdc_lead_contact = 'contact' AND sfdc_account.account_name <> '[[unknown]]' THEN sfdc_account.account_name
        WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.company =  '[[unknown]]' THEN NULL
        WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.company <>  '[[unknown]]' THEN sfdc_lead.company
      END                                                                                                                   AS company_name,
      crm_person.title                                                                                                      AS job_title,
      sales_segment.sales_segment_name                                                                                      AS sales_segment,
      CASE
        WHEN sfdc_lead_contact = 'contact' THEN sfdc_contact.mailing_country
        ELSE sfdc_lead.country
      END                                                                                                                   AS country,
      CASE
        WHEN sfdc_lead_contact = 'contact' THEN sfdc_contact.created_date
        ELSE sfdc_lead.created_date
      END                                                                                                                   AS sfdc_created_date,
      crm_person.has_opted_out_email                                                                                        AS opted_out_salesforce,
      (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY sfdc_created_date DESC))                                           AS record_number

    FROM crm_person
    LEFT JOIN sfdc_contact
      ON sfdc_contact.contact_id = crm_person.sfdc_record_id
    LEFT JOIN sfdc_lead
      ON sfdc_lead.lead_id = sfdc_record_id
    LEFT JOIN sfdc_account
      ON sfdc_account.account_id = sfdc_contact.account_id
    LEFT JOIN crm_account
      ON crm_account.account_dim_crm_account_id = crm_person.dim_crm_account_id
    JOIN sales_segment
      ON sales_segment.dim_sales_segment_id = crm_account.account_dim_sales_segment_id
    WHERE  email_address IS NOT NULL
      AND email_address <> ''
    QUALIFY record_number = 1

), gitlab_dotcom AS (

    SELECT
      notification_email                                                                                                    AS email_address,
      user_id                                                                                                               AS user_id,
      SPLIT_PART(users_name,' ',1)                                                                                          AS first_name,
      SPLIT_PART(users_name,' ',2)                                                                                          AS last_name,
      user_name                                                                                                             AS user_name,
      organization                                                                                                          AS company_name,
      role                                                                                                                  AS job_title,
      created_at                                                                                                            AS created_date,
      confirmed_at                                                                                                          AS confirmed_date,
      state                                                                                                                 AS active_state,
      last_sign_in_at                                                                                                       AS last_login_date,
      is_email_opted_in                                                                                                     AS email_opted_in,
      (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY created_date DESC))                                           AS record_number
    FROM gitlab_users
    WHERE email_address IS NOT NULL
      AND email_address <> ''
    QUALIFY record_number = 1

), customer_db AS (

    SELECT
      customer_email                                                                                                        AS email_address,
      customer_id                                                                                                           AS customer_id,
      customer_first_name                                                                                                   AS first_name,
      customer_last_name                                                                                                    AS last_name,
      company                                                                                                               AS company_name,
      country                                                                                                               AS country,
      customer_created_at                                                                                                   AS created_date,
      confirmed_at                                                                                                          AS confirmed_date,
      company_size                                                                                                          AS market_segment,
      last_sign_in_at                                                                                                       AS last_login_date,
      (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY created_date DESC))                                           AS record_number
    FROM customer_db_source
    WHERE email_address IS NOT NULL
      AND email_address <> ''
    QUALIFY record_number = 1

), zuora AS (

    SELECT
      zuora_contact_source.work_email                                                                                       AS email_address,
      zuora_contact_source.contact_id                                                                                       AS contact_id,
      zuora_contact_source.first_name                                                                                       AS first_name,
      zuora_contact_source.last_name                                                                                        AS last_name,
      zuora_account_source.account_name                                                                                     AS company_name,
      zuora_contact_source.country                                                                                          AS country,
      zuora_contact_source.created_date                                                                                     AS created_date,
      CASE
        WHEN zuora_contact_source.is_deleted = TRUE THEN 'Inactive'
        ELSE 'Active'
      END                                                                                                                   AS active_state,
      (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY zuora_contact_source.created_date DESC))                                           AS record_number
    FROM zuora_contact_source
    JOIN zuora_account_source
      ON zuora_account_source.account_id = zuora_contact_source.account_id
    WHERE email_address IS NOT NULL
      AND email_address <> ''
    QUALIFY record_number = 1

), emails AS (

    SELECT DISTINCT
      email_address
    FROM sfdc

    UNION

    SELECT DISTINCT
      email_address
    FROM gitlab_dotcom

    UNION

    SELECT DISTINCT
      email_address
    FROM customer_db

    UNION

    SELECT DISTINCT
      email_address
    FROM zuora

), final AS (

    SELECT
      emails.email_address,
      COALESCE(zuora.first_name, sfdc.first_name, customer_db.first_name, gitlab_dotcom.first_name)                      AS first_name,
      COALESCE(zuora.last_name, sfdc.last_name, customer_db.last_name, gitlab_dotcom.last_name)                          AS last_name,
      gitlab_dotcom.user_name                                                                                            AS gitlab_user_name,
      COALESCE(zuora.company_name,  sfdc.company_name, customer_db.company_name, gitlab_dotcom.company_name)             AS company_name,
      COALESCE(sfdc.job_title, gitlab_dotcom.job_title)                                                                  AS job_title,
      COALESCE(zuora.country, sfdc.country, customer_db.country)                                                         AS country,
      sfdc.sales_segment                                                                                                 AS sfdc_parent_sales_segment,
      CASE
        WHEN sfdc.email_address IS NOT NULL THEN TRUE
        ELSE FALSE
      END                                                                                                                AS is_sfdc_lead_contact,
      sfdc.sfdc_lead_contact,
      sfdc.sfdc_created_date                                                                                             AS sfdc_created_date,
      sfdc.opted_out_salesforce                                                                                          AS is_sfdc_opted_out,
      CASE
        WHEN gitlab_dotcom.email_address IS NOT NULL THEN TRUE
        ELSE FALSE
      END                                                                                                                AS is_gitlab_dotcom_user,
      gitlab_dotcom.user_id                                                                                              AS gitlab_dotcom_user_id,
      gitlab_dotcom.created_date                                                                                         AS gitlab_dotcom_created_date,
      gitlab_dotcom.confirmed_date                                                                                       AS gitlab_dotcom_confirmed_date,
      gitlab_dotcom.active_state                                                                                         AS gitlab_dotcom_active_state,
      gitlab_dotcom.last_login_date                                                                                      AS gitlab_dotcom_last_login_date,
      gitlab_dotcom.email_opted_in                                                                                       AS gitlab_dotcom_email_opted_in,
      CASE
        WHEN customer_db.email_address IS NOT NULL THEN TRUE
        ELSE FALSE
      END                                                                                                                AS is_customer_db_user,
      customer_db.customer_id                                                                                            AS customer_db_customer_id,
      customer_db.created_date                                                                                           AS customer_db_created_date,
      customer_db.confirmed_date                                                                                         AS customer_db_confirmed_date,
      CASE
        WHEN zuora.email_address IS NOT NULL THEN TRUE
        ELSE FALSE
      END                                                                                                                AS is_zuora_billing_contact,
      zuora.contact_id                                                                                                   AS zuora_contact_id,
      zuora.created_date                                                                                                 AS zuora_created_date,
      zuora.active_state                                                                                                 AS zuora_active_state

    FROM emails
    LEFT JOIN sfdc
      ON sfdc.email_address = emails.email_address
    LEFT JOIN gitlab_dotcom
      ON gitlab_dotcom.email_address = emails.email_address
    LEFT JOIN customer_db
      ON customer_db.email_address = emails.email_address
    LEFT JOIN zuora
      ON zuora.email_address = emails.email_address

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rmistry",
    updated_by="@rmistry",
    created_date="2021-01-19",
    updated_date="2021-01-20"
) }}
