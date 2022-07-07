{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('company_domain_account','bdg_company_domain_account'),
    ('company_domain','bdg_company_domain'),
    ('crm_account','dim_crm_account'),
    ('company','wk_dim_company'),
    ('namespace_order_subscription_monthly','bdg_namespace_order_subscription_monthly'),
    ('namespaces','dim_namespace'),
    ('users','dim_user')
])}},

user_namespace_account_company AS (
  SELECT *
  FROM {{ ref('wk_fct_user_namespace_account_company') }} 
  WHERE is_billable = TRUE
),

domain_matching AS (

  SELECT 
    --user_namespace_account_company.*,
    user_namespace_account_company.user_id,
    user_namespace_account_company.is_creator,
    user_namespace_account_company.is_owner,
    namespaces.ultimate_parent_namespace_id,
    company.source_company_id,
    IFF(company_domain.company_id IS NOT NULL, TRUE, FALSE) AS is_company_domain,
    CASE
      WHEN user_namespace_account_company.crm_account_id IS NOT NULL THEN 'Direct Match'
      WHEN company_domain_account.dim_crm_account_id IS NOT NULL AND
            ARRAY_CONTAINS(users.email_domain::VARIANT, company_domain_account.domain_list) THEN 'Domain Match'
      ELSE 'No Match'
    END AS user_account_type,
    CASE user_account_type
      WHEN 'Direct Match' THEN user_namespace_account_company.crm_account_id
      WHEN 'Domain Match' THEN company_domain_account.dim_crm_account_id
      ELSE NULL
    END AS account_id
  FROM user_namespace_account_company
  LEFT JOIN namespaces
    ON user_namespace_account_company.namespace_id = namespaces.dim_namespace_id
  LEFT JOIN company
    ON user_namespace_account_company.company_id = company.company_id
  LEFT JOIN users
    ON user_namespace_account_company.user_id = users.dim_user_id
  LEFT JOIN company_domain_account
    ON company.source_company_id = company_domain_account.company_id
    AND users.email_domain = company_domain_account.email_domain
  LEFT JOIN company_domain
    ON users.email_domain = company_domain.email_domain
  QUALIFY ROW_NUMBER() OVER (PARTITION BY namespaces.ultimate_parent_namespace_id,user_namespace_account_company.user_id ORDER BY user_namespace_account_company.access_level ) = 1
),

top_namespace_domain AS (
-- Find the top used user email domains for the ultimate parent namespace for matching
  SELECT
    domain_matching.ultimate_parent_namespace_id,
    users.email_domain,
    IFF(users.email_domain_classification IS NULL, TRUE, FALSE) AS is_business_email,
    COUNT(DISTINCT user_id) AS number_of_users,
    ROW_NUMBER() OVER (PARTITION BY domain_matching.ultimate_parent_namespace_id ORDER BY number_of_users DESC) AS domain_rank
  FROM domain_matching
  LEFT JOIN users
    ON domain_matching.user_id = users.dim_user_id
  WHERE is_business_email = TRUE
  GROUP BY 1, 2, 3
  QUALIFY domain_rank = 1

),

namespace_domain_account AS (
  SELECT DISTINCT
    top_namespace_domain.ultimate_parent_namespace_id,
    top_namespace_domain.email_domain,
    company_domain_account.dim_crm_account_id,
    account_domain_rank,
    ROW_NUMBER() OVER (PARTITION BY 
      top_namespace_domain.ultimate_parent_namespace_id,
      top_namespace_domain.email_domain 
        ORDER BY account_domain_rank DESC, 
        company_domain_account.dim_crm_account_id ) AS namespace_account_rank
  FROM top_namespace_domain
  INNER JOIN company_domain_account
    ON top_namespace_domain.email_domain = company_domain_account.email_domain
  QUALIFY namespace_account_rank = 1


),

namespace_company AS (
-- Measures and applies logic for determining the best match for a namespace and company
  SELECT
    ultimate_parent_namespace_id,
    source_company_id,
    CASE
      WHEN source_company_id IS NOT NULL THEN 1
      ELSE 0
    END AS has_company, 
    MAX(IFF(is_creator, 1, 0)) AS has_creator,
    MAX(CASE
          WHEN is_creator = TRUE AND is_company_domain = TRUE THEN 1
          ELSE 0
        END) AS is_creator_direct_match,  -- ?
    COUNT(DISTINCT IFF(is_owner, user_id, NULL)) AS total_owners,
    COUNT(DISTINCT user_id) AS total_users, 
    --COUNT(DISTINCT source_company_id) OVER (PARTITION BY namespace_id) AS total_num_company, -- ?
    SUM(total_owners) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners,
    SUM(total_users) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users, 
    SUM(IFF(source_company_id IS NOT NULL, total_owners, 0))
        OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners_qualified,
    SUM(IFF(source_company_id IS NOT NULL, total_users, 0))
        OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users_qualified,
    SUM(IFF(source_company_id IS NOT NULL, has_creator, 0))
        OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_creator_qualified,  
    ROW_NUMBER() OVER ( PARTITION BY ultimate_parent_namespace_id ORDER BY has_company DESC, has_creator DESC, total_owners DESC,total_users DESC) AS rn,
    ROW_NUMBER() OVER ( PARTITION BY ultimate_parent_namespace_id ORDER BY has_company DESC, total_owners DESC,total_users DESC) AS rn_owner,
    ROW_NUMBER() OVER ( PARTITION BY ultimate_parent_namespace_id ORDER BY has_company DESC,total_users DESC) AS rn_user, 
    NTH_VALUE(total_owners, 1)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_company DESC, total_owners DESC,total_users DESC) AS owner_count_1st, -- ?
    NTH_VALUE(total_owners, 2)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_company DESC, total_owners DESC,total_users DESC) AS owner_count_2nd,  --?
    NTH_VALUE(total_users, 1)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_company DESC,total_users DESC) AS users_count_1st, -- ?
    NTH_VALUE(total_users, 2)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_company DESC,total_users DESC) AS users_count_2nd, --?
    IFF(total_namespace_owners_qualified > 0, total_owners / total_namespace_owners_qualified,
        0) AS owner_percent_qualified,
    IFF(total_namespace_users_qualified > 0, total_users / total_namespace_users_qualified,
        0) AS users_percent_qualified,
    IFF(total_namespace_owners > 0, total_owners / total_namespace_owners, 0) AS owner_percent,
    IFF(total_namespace_users > 0, total_users / total_namespace_users, 0) AS users_percent,

    -- Matching logic ratios derived from an evaluation of the results of the query
    IFF(rn = 1
      AND has_company = 1
      AND (
            (has_creator = 1 AND rn_owner = 1 AND rn_user = 1
              AND owner_percent_qualified > 0.5 AND users_percent_qualified > 0.5
              AND owner_percent > 0.2 AND users_percent > 0.2
              )
            OR
            (has_creator = 0 AND rn_owner = 1 AND rn_user = 1 AND total_namespace_creator_qualified = 0
              AND owner_percent_qualified > 0.6 AND users_percent_qualified > 0.6
              AND owner_percent > 0.33 AND users_percent > 0.33
              )
          )
      ,TRUE,FALSE) AS is_best_match
  FROM domain_matching
  GROUP BY 1, 2, 3

  ),


namespace_account AS (

  SELECT
    ultimate_parent_namespace_id,
    account_id,
    CASE
      WHEN account_id IS NOT NULL THEN 1
      ELSE 0
    END AS has_account,                                                                                                          -- namepace and account level detail
    MAX(IFF(is_creator, 1, 0)) AS has_creator,
    MAX(CASE
          WHEN is_creator = TRUE AND user_account_type = 'Direct Match' THEN 1 --?
          ELSE 0
        END) AS is_creator_direct_match, --?
    COUNT(DISTINCT IFF(is_owner, user_id, NULL)) AS total_owners,
    COUNT(DISTINCT user_id) AS total_users,                                                                                      -- overall namespace level
    COUNT(DISTINCT account_id) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_num_accounts, -- ?
    SUM(total_owners) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners,
    SUM(total_users) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users,                                                  -- overall namespace level but only when account id is present
    SUM(IFF(account_id IS NOT NULL, total_owners, 0))
        OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners_qualified,
    SUM(IFF(account_id IS NOT NULL, total_users, 0))
        OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users_qualified,                                                     -- row number
    ROW_NUMBER() OVER ( PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC, has_creator DESC, total_owners DESC,total_users DESC) AS rn,
    ROW_NUMBER() OVER ( PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC, total_owners DESC,total_users DESC) AS rn_owner,
    ROW_NUMBER() OVER ( PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC,total_users DESC) AS rn_user,                        --Nth value
    NTH_VALUE(total_owners, 1)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC, total_owners DESC,total_users DESC) AS owner_count_1st,  --?
    NTH_VALUE(total_owners, 2)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC, total_owners DESC,total_users DESC) AS owner_count_2nd, --? --pulls the count of the 2nd most
    NTH_VALUE(total_users, 1)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC,total_users DESC) AS users_count_1st,
    NTH_VALUE(total_users, 2)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC,total_users DESC) AS users_count_2nd,
    NTH_VALUE(has_account, 2)
              OVER (PARTITION BY ultimate_parent_namespace_id ORDER BY has_account DESC,total_users DESC) AS users_count_2nd_has_account,        -- % calculations
    IFF(total_namespace_owners_qualified > 0, total_owners / total_namespace_owners_qualified,
        0) AS owner_percent_qualified,
    IFF(total_namespace_users_qualified > 0, total_users / total_namespace_users_qualified,
        0) AS users_percent_qualified, -- ?
    IFF(total_namespace_owners > 0, total_owners / total_namespace_owners, 0) AS owner_percent, -- ?
    IFF(total_namespace_users > 0, total_users / total_namespace_users, 0) AS users_percent, -- ?
    IFF(
      rn_user = 1
      AND total_users >= 1
      AND has_account = 1
      AND (COALESCE(users_count_2nd_has_account, 0) = 0
        OR (users_count_1st > COALESCE(users_count_2nd, 0)
          AND COALESCE(users_count_2nd_has_account, 1) = 1)
          ),TRUE,FALSE
      ) AS is_most_matched_account,
    IFF(rn = 1
      AND has_account = 1
      AND (
            (has_creator = 1 AND rn_owner = 1 AND owner_percent_qualified > 0.5)
            OR
            (has_creator = 0 AND rn_owner = 1 AND owner_percent_qualified > 0.66)
          )
      ,TRUE,FALSE) AS is_best_match
  FROM domain_matching
  GROUP BY 1, 2, 3
),

subscription_accounts AS (

  SELECT DISTINCT
    dim_namespace_id AS namespace_id,
    dim_crm_account_id
  FROM namespace_order_subscription_monthly
  WHERE dim_crm_account_id IS NOT NULL
    AND dim_namespace_id IS NOT NULL
    AND snapshot_month <= DATE_TRUNC('month', CURRENT_DATE())
    QUALIFY MAX(snapshot_month) OVER (PARTITION BY dim_namespace_id) = snapshot_month
    AND ROW_NUMBER()  OVER (PARTITION BY dim_namespace_id ORDER BY order_start_date DESC) = 1

),

direct_company_account AS (
  
  SELECT
    subscription_accounts.namespace_id,
    dim_crm_account.crm_account_zoom_info_dozisf_zi_id AS dozisf_zi_id,
    dim_crm_account.dim_crm_account_id,
    dim_crm_account.dim_parent_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_crm_account.parent_crm_account_name,
    wk_dim_company.company_id,
    wk_dim_company.source_company_id
  FROM subscription_accounts
  LEFT JOIN prod.restricted_safe_common.dim_crm_account
    ON subscription_accounts.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN prod.workspace_marketing.wk_dim_company
    ON dim_crm_account.crm_account_zoom_info_dozisf_zi_id = wk_dim_company.company_id
),

source_company_accounts AS (

  SELECT
    source_company_id,
    MAX(c.dim_crm_account_id) AS account_id,
    MAX(c.dim_parent_crm_account_id) AS parent_account_id,
    COUNT(DISTINCT c.dim_crm_account_id) AS no_a,
    COUNT(DISTINCT c.dim_parent_crm_account_id) AS no_upa,
    ARRAY_AGG(DISTINCT CONCAT(c.dim_crm_account_id, ':', c.crm_account_name)) AS list_of_accounts,
    ARRAY_AGG(DISTINCT CONCAT(c.dim_parent_crm_account_id, ':', c.parent_crm_account_name)) AS list_of_upa

  FROM crm_account c
  LEFT JOIN company zc
    ON c.crm_account_zoom_info_dozisf_zi_id = zc.company_id
  GROUP BY 1
),

mart AS (

  SELECT
    ns.namespace_type,
    ns.creator_id,
    ns.visibility_level,
    ns.dim_namespace_id AS namespace_id,
    ns.gitlab_plan_title,
    ns.gitlab_plan_is_paid,
    ns.is_setup_for_company,
    ns.created_at,
    ns.namespace_is_ultimate_parent,
    ns.namespace_is_internal,
    ns.namespace_creator_is_blocked,
    CASE
      WHEN du.dim_user_id IS NULL THEN 'Missing'
      ELSE COALESCE(du.email_domain_classification, 'Business')
    END AS email_domain_type,
    m.dim_crm_account_id AS actual_account_id,
    m.source_company_id AS actual_company_id,
    IFF(c.ultimate_parent_namespace_id IS NOT NULL, c.source_company_id, NULL) AS predicted_company_id,
    IFF(a.ultimate_parent_namespace_id IS NOT NULL, a.account_id, NULL) AS predicted_account_id,
    COALESCE(actual_account_id, predicted_account_id) AS namespace_account_id,
    COALESCE(actual_company_id, predicted_company_id) AS namespace_company_id,

    COALESCE(IFF(s.no_a = 1, s.account_id, NULL), /*ns1.account_id,*/ namespace_domain_account.dim_crm_account_id) AS zi_linked_account,
    COALESCE(namespace_account_id, zi_linked_account) AS combined_account_id,
    CASE
      WHEN actual_account_id IS NOT NULL THEN 'actual_account'
      WHEN namespace_account_id IS NOT NULL THEN 'Predicted_account'
      WHEN zi_linked_account IS NOT NULL THEN 'Zi_linked_account'
      WHEN s.no_a IS NOT NULL THEN 'Zi_linked_Multi_account'
      WHEN predicted_company_id IS NOT NULL THEN 'Zi_linked_but_no_account_match'
      ELSE 'None'
    END AS match_account_type, 
    s.no_a,
    s.no_upa,
    s.list_of_accounts,
    s.list_of_upa
  FROM namespaces ns
  LEFT JOIN users du
    ON du.dim_user_id = ns.creator_id
  LEFT JOIN direct_company_account m
    ON m.namespace_id = ns.dim_namespace_id
  LEFT JOIN namespace_company c
    ON c.ultimate_parent_namespace_id = ns.dim_namespace_id
    AND c.is_best_match = TRUE
  LEFT JOIN namespace_account a
    ON a.ultimate_parent_namespace_id = ns.dim_namespace_id
    AND a.is_best_match = TRUE
  LEFT JOIN namespace_domain_account
      ON ns.dim_namespace_id = namespace_domain_account.ultimate_parent_namespace_id
  LEFT JOIN source_company_accounts s
    ON s.source_company_id = COALESCE(m.source_company_id,c.source_company_id)

  )

  SELECT *
  FROM mart