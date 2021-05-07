WITH dim_billing_account AS (

    SELECT *
    FROM {{ ref('dim_billing_account') }}

), dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), dim_location AS (

    SELECT *
    FROM {{ ref('dim_location_country') }}

), dim_product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}

), fct_usage_ping_payload AS (

    SELECT *
    FROM {{ ref('fct_usage_ping_payload') }}

), dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}

), joined AS (

    SELECT
      fct_usage_ping_payload.*,
      dim_crm_account.dim_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.crm_account_billing_country,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory,
      dim_date.date_actual,
      dim_date.first_day_of_month,
      dim_date.fiscal_quarter_name_fy,
      dim_location.country_name,
      dim_location.iso_2_country_code
    FROM fct_usage_ping_payload
    LEFT JOIN dim_subscription
      ON fct_usage_ping_payload.dim_subscription_id = dim_subscription.dim_subscription_id
    LEFT JOIN dim_billing_account
      ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_date
      ON fct_usage_ping_payload.date_id = dim_date.date_id
    LEFT JOIN dim_location
      ON fct_usage_ping_payload.location_id = dim_location.dim_location_country_id

), renamed AS (

    SELECT
      -- keys
      dim_usage_ping_id,
      host_name,
      dim_instance_id,

      -- date info
      date_id,
      ping_created_at,
      ping_created_at_date,
      ping_created_at_month,
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
      dim_billing_account_id,
      dim_crm_account_id,
      crm_account_name,
      crm_account_billing_country,
      dim_parent_crm_account_id,
      parent_crm_account_billing_country,
      parent_crm_account_industry,
      parent_crm_account_owner_team,
      parent_crm_account_sales_segment,
      parent_crm_account_sales_territory,

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
      instance_user_count,
      gitpod_enabled
    FROM joined
    WHERE hostname NOT IN ('staging.gitlab.com', 'dr.gitlab.com')

)

SELECT *
FROM renamed
