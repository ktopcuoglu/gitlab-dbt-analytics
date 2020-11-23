WITH dim_billing_accounts AS (

    SELECT *
    FROM {{ ref('dim_billing_accounts') }}

), dim_crm_accounts AS (

    SELECT *
    FROM {{ ref('dim_crm_accounts') }}

), dim_dates AS (

    SELECT *
    FROM {{ ref('dim_dates') }}

), dim_location AS (

    SELECT *
    FROM {{ ref('dim_location') }}

), dim_product_details AS (

    SELECT *
    FROM {{ ref('dim_product_details') }}

), fct_usage_ping_payloads AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payloads') }}

), flattened AS (

    SELECT
      usage_ping_id,
      f.value AS product_details_id
    FROM fct_usage_ping_payloads,
      LATERAL FLATTEN(input => fct_usage_ping_payloads.array_product_details_id) AS f  

), product_details AS (

    SELECT
      usage_ping_id,
      ARRAY_AGG(DISTINCT product_rate_plan_name) AS product_rate_plans,
      ARRAY_AGG(DISTINCT product_category)       AS product_categories
    FROM flattened
    LEFT JOIN dim_product_details
      ON flattened.product_details_id = dim_product_details.product_details_id  
    GROUP BY 1

), joined AS (

    SELECT
      fct_usage_ping_payloads.*,
      dim_crm_accounts.crm_account_id,
      dim_crm_accounts.crm_account_name,
      dim_crm_accounts.crm_account_country,
      dim_crm_accounts.ultimate_parent_account_id,
      dim_crm_accounts.ultimate_parent_account_segment,
      dim_crm_accounts.ultimate_parent_billing_country,
      dim_crm_accounts.ultimate_parent_industry,
      dim_crm_accounts.ultimate_parent_account_owner_team,
      dim_crm_accounts.ultimate_parent_territory,
      dim_dates.date_actual,
      dim_dates.first_day_of_month,
      dim_dates.fiscal_quarter_name_fy,
      dim_location.country_name,
      dim_location.iso_2_country_code,
      product_details.product_rate_plans,
      product_details.product_categories
    FROM fct_usage_ping_payloads
    LEFT JOIN dim_billing_accounts
      ON fct_usage_ping_payloads.account_id = dim_billing_accounts.billing_account_id
    LEFT JOIN dim_crm_accounts
      ON dim_billing_accounts.crm_account_id = dim_crm_accounts.crm_account_id
    LEFT JOIN dim_dates
      ON fct_usage_ping_payloads.date_id = dim_dates.date_id
    LEFT JOIN dim_location
      ON fct_usage_ping_payloads.location_id = dim_location.location_id
    LEFT JOIN product_details
      ON fct_usage_ping_payloads.usage_ping_id = product_details.usage_ping_id       

), renamed AS (

    SELECT
      -- keys
      usage_ping_id,
      hostname,
      host_id,
      uuid,

      -- date info
      date_id,
      created_at              AS ping_created_at,
      recorded_at             AS ping_recorded_at,    
      date_actual             AS ping_date,
      first_day_of_month      AS ping_month,
      fiscal_quarter_name_fy  AS ping_fiscal_quarter,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY uuid, ping_month 
          ORDER BY usage_ping_id DESC
          ) = 1, TRUE, FALSE) AS is_last_ping_in_month,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY uuid, fiscal_quarter_name_fy
          ORDER BY usage_ping_id DESC
          ) = 1, TRUE, FALSE) AS is_last_ping_in_quarter,

      -- customer info
      account_id,
      crm_account_id,
      crm_account_name, 
      crm_account_country,
      ultimate_parent_account_id,
      ultimate_parent_billing_country,
      ultimate_parent_industry,
      ultimate_parent_account_owner_team,
      ultimate_parent_account_segment,
      ultimate_parent_territory,

      -- product info
      license_md5,
      is_trial                AS ping_is_trial_license,
      product_tier            AS ping_product_tier,
      product_categories,
      product_rate_plans, 
      subscription_id,

      -- location info
      location_id,
      country_name            AS ping_country_name,
      iso_2_country_code      AS ping_country_code,

      -- metadata
      IFF(uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f', 'SaaS', 'Self-Managed')
                              AS ping_source,
      edition,
      version,
      is_pre_release,
      instance_user_count
    FROM joined
    WHERE hostname NOT IN ('staging.gitlab.com', 'dr.gitlab.com')  

)

SELECT *
FROM renamed
