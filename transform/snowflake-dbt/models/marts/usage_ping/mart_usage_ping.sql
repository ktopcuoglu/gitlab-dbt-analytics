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

), dim_license AS (

    SELECT *
    FROM {{ ref('dim_license') }}

), joined AS (

    SELECT
      fct_usage_ping_payload.*,
      dim_billing_account.dim_billing_account_id,
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
      dim_location.iso_2_country_code,
      dim_license.license_md5
    FROM fct_usage_ping_payload
    LEFT JOIN dim_subscription
      ON fct_usage_ping_payload.dim_subscription_id = dim_subscription.dim_subscription_id
    LEFT JOIN dim_billing_account
      ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_date
      ON fct_usage_ping_payload.dim_date_id = dim_date.date_id
    LEFT JOIN dim_location
      ON fct_usage_ping_payload.dim_location_country_id = dim_location.dim_location_country_id
    LEFT JOIN dim_license
      ON fct_usage_ping_payload.dim_license_id = dim_license.dim_license_id

), renamed AS (

    SELECT
      -- keys
      dim_usage_ping_id,
      host_name,
      dim_instance_id,

      -- date info
      dim_date_id,
      ping_created_at,
      ping_created_at_date,
      ping_created_at_month,
      fiscal_quarter_name_fy  AS ping_fiscal_quarter,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY dim_instance_id, ping_created_at_month
          ORDER BY dim_usage_ping_id DESC
          ) = 1, TRUE, FALSE) AS is_last_ping_in_month,
      IFF(ROW_NUMBER() OVER (
          PARTITION BY dim_instance_id, fiscal_quarter_name_fy
          ORDER BY dim_usage_ping_id DESC
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
      --is_trial                AS ping_is_trial_license,
      product_tier            AS ping_product_tier, -- might rename it in the payload model

      -- location info
      dim_location_country_id,
      country_name            AS ping_country_name,
      iso_2_country_code      AS ping_country_code,

      -- metadata
      usage_ping_delivery_type,
      main_edition,
      major_minor_version,
      version_is_prerelease,
      instance_user_count
    FROM joined
    WHERE host_name NOT IN ('staging.gitlab.com', 'dr.gitlab.com')

)

SELECT *
FROM renamed
