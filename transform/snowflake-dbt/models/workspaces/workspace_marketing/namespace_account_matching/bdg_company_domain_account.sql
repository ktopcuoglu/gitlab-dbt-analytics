{{ simple_cte([
    ('crm_person','dim_crm_person'),
    ('crm_account','dim_crm_account')
])}},
  
  
person_domains AS (

  SELECT DISTINCT
    crm_person.dim_crm_account_id,
    crm_person.email_domain,
    crm_account.crm_account_zoom_info_dozisf_zi_id AS company_id,
    IFF(crm_person.email_domain_type = 'Business email domain', TRUE, FALSE) AS is_business_email,
    COUNT(DISTINCT crm_person.email_domain) OVER (PARTITION BY company_id) AS number_of_domains,
    COUNT(DISTINCT crm_person.dim_crm_person_id)
          OVER (PARTITION BY company_id) AS number_of_persons,
    COUNT(DISTINCT crm_person.dim_crm_person_id)
          OVER (PARTITION BY crm_person.dim_crm_account_id) AS number_of_account_persons,
    COUNT(DISTINCT crm_person.dim_crm_person_id)
          OVER (PARTITION BY crm_person.dim_crm_account_id,crm_person.email_domain) AS number_of_account_domain_persons,
    COUNT(DISTINCT crm_person.dim_crm_person_id)
          OVER (PARTITION BY company_id,crm_person.email_domain) AS number_of_domain_persons
  FROM crm_person
  LEFT JOIN crm_account
    ON crm_person.dim_crm_account_id = crm_account.dim_crm_account_id

),

domain_ranks AS (
  SELECT
    *,
    number_of_account_domain_persons / number_of_account_persons AS account_domain_user_ratio,
    DENSE_RANK() OVER (PARTITION BY company_id ORDER BY number_of_domain_persons DESC,email_domain ) AS domain_rank
  FROM person_domains
  WHERE is_business_email
)


SELECT DISTINCT
  company_id,
  email_domain,
  LAST_VALUE(account_domain_user_ratio)
              OVER (PARTITION BY company_id,email_domain ORDER BY number_of_domain_persons, number_of_account_persons,dim_crm_account_id) AS account_domain_rank,
  LAST_VALUE(dim_crm_account_id)
              OVER (PARTITION BY company_id,email_domain ORDER BY number_of_domain_persons, number_of_account_persons,dim_crm_account_id) AS dim_crm_account_id,
  TO_ARRAY(email_domain) AS domain_list
FROM domain_ranks
WHERE domain_rank < 4