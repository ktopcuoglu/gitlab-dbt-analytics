{{ simple_cte([
    ('user_company','wk_bdg_user_company'),
    ('company','wk_dim_company'),
    ('users','dim_user')
])}},
    
company_domains AS (

  SELECT
    company.source_company_id AS company_id,
    users.email_domain,
    IFF(users.email_domain_classification IS NULL, TRUE, FALSE) AS is_business_domain,
    COUNT(DISTINCT user_company.dim_user_id)
          OVER (PARTITION BY users.email_domain) AS number_of_domain_users,
    COUNT(DISTINCT users.email_domain)
          OVER (PARTITION BY company.source_company_id) AS number_of_domains,
    COUNT(DISTINCT user_company.dim_user_id)
          OVER (PARTITION BY company.source_company_id, users.email_domain) AS _number_of_domain_users,
    COUNT(DISTINCT company.source_company_id)
          OVER (PARTITION BY users.email_domain) AS number_of_domain_companies
  FROM user_company
  LEFT JOIN users
    ON user_company.gitlab_dotcom_user_id = users.dim_user_id
  LEFT JOIN company
    ON user_company.company_id = company.company_id
),

ranks AS (
  SELECT DISTINCT
    *,
    DENSE_RANK() OVER (PARTITION BY email_domain ORDER BY _number_of_domain_users DESC,company_id ) AS company_rank,
    DENSE_RANK() OVER (PARTITION BY company_id ORDER BY number_of_domain_users DESC,email_domain ) AS domain_rank
  FROM company_domains
  WHERE is_business_domain
)

  SELECT 
    company_id,
    email_domain
  FROM ranks
  WHERE TRUE
    AND company_rank = 1
    AND domain_rank < 4
  ORDER BY 1, 2